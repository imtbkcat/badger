/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package table

import (
	"encoding/binary"
	"math"
	"os"
	"reflect"
	"unsafe"

	"github.com/coocood/badger/fileutil"
	"github.com/coocood/badger/options"
	"github.com/coocood/badger/y"
	"github.com/coocood/bbloom"
	"github.com/dgryski/go-farm"
	"golang.org/x/time/rate"
)

const restartInterval = 256 // Might want to change this to be based on total size instead of numKeys.

type header struct {
	baseLen uint16 // Overlap with base key.
	diffLen uint16 // Length of the diff.
}

// Encode encodes the header.
func (h header) Encode() []byte {
	var b [4]byte
	*(*header)(unsafe.Pointer(&b[0])) = h
	return b[:]
}

// Decode decodes the header.
func (h *header) Decode(buf []byte) {
	*h = *(*header)(unsafe.Pointer(&buf[0]))
}

const headerSize = 4

// Builder is used in building a table.
type Builder struct {
	counter int // Number of keys written for the current block.

	w          *fileutil.DirectWriter
	buf        []byte
	writtenLen int

	baseKeysBuf     []byte
	baseKeysEndOffs []uint32

	blockBaseKey    []byte // Base key for the current block.
	blockBaseOffset uint32 // Offset for the current block.

	blockEndOffsets []uint32 // Base offsets of every block.

	// end offsets of every entry within the current block being built.
	// The offsets are relative to the start of the block.
	entryEndOffsets []uint32

	hashEntries []hashEntry
	bloomFpr    float64
	isExternal  bool
	opt         options.TableBuilderOptions
}

// NewTableBuilder makes a new TableBuilder.
// If the limiter is nil, the write speed during table build will not be limited.
func NewTableBuilder(f *os.File, limiter *rate.Limiter, level int, opt options.TableBuilderOptions) *Builder {
	t := float64(opt.LevelSizeMultiplier)
	fprBase := math.Pow(t, 1/(t-1)) * opt.LogicalBloomFPR * (t - 1)
	levelFactor := math.Pow(t, float64(opt.MaxLevels-level))

	return &Builder{
		w:           fileutil.NewDirectWriter(f, opt.WriteBufferSize, limiter),
		buf:         make([]byte, 0, 4*1024),
		baseKeysBuf: make([]byte, 0, 4*1024),
		hashEntries: make([]hashEntry, 0, 4*1024),
		bloomFpr:    fprBase / levelFactor,
		opt:         opt,
	}
}

func NewExternalTableBuilder(f *os.File, limiter *rate.Limiter, opt options.TableBuilderOptions) *Builder {
	return &Builder{
		w:           fileutil.NewDirectWriter(f, opt.WriteBufferSize, limiter),
		buf:         make([]byte, 0, 4*1024),
		baseKeysBuf: make([]byte, 0, 4*1024),
		hashEntries: make([]hashEntry, 0, 4*1024),
		bloomFpr:    opt.LogicalBloomFPR,
		isExternal:  true,
		opt:         opt,
	}
}

// Reset this builder with new file.
func (b *Builder) Reset(f *os.File) {
	b.resetBuffers()
	b.w.Reset(f)
}

func (b *Builder) resetBuffers() {
	b.counter = 0
	b.buf = b.buf[:0]
	b.writtenLen = 0
	b.baseKeysBuf = b.baseKeysBuf[:0]
	b.baseKeysEndOffs = b.baseKeysEndOffs[:0]
	b.blockBaseKey = b.blockBaseKey[:0]
	b.blockBaseOffset = 0
	b.blockEndOffsets = b.blockEndOffsets[:0]
	b.entryEndOffsets = b.entryEndOffsets[:0]
	b.hashEntries = b.hashEntries[:0]
}

// Close closes the TableBuilder.
func (b *Builder) Close() {}

// Empty returns whether it's empty.
func (b *Builder) Empty() bool { return b.writtenLen+len(b.buf) == 0 }

// keyDiff returns a suffix of newKey that is different from b.blockBaseKey.
func (b Builder) keyDiff(newKey []byte) []byte {
	for i := 0; i < len(newKey) && i < len(b.blockBaseKey); i++ {
		if newKey[i] != b.blockBaseKey[i] {
			return newKey[i:]
		}
	}
	return newKey
}

func (b *Builder) addHelper(key []byte, v y.ValueStruct) {
	// Add key to bloom filter.
	if len(key) > 0 {
		keyNoTs := key
		if !b.isExternal {
			keyNoTs = y.ParseKey(key)
		}
		keyHash := farm.Fingerprint64(keyNoTs)
		// It is impossible that a single table contains 16 million keys.
		y.Assert(len(b.baseKeysEndOffs) < maxBlockCnt)
		b.hashEntries = append(b.hashEntries, hashEntry{keyHash, uint16(len(b.baseKeysEndOffs)), uint8(b.counter)})
	}

	// diffKey stores the difference of key with blockBaseKey.
	var diffKey []byte
	if len(b.blockBaseKey) == 0 {
		// Make a copy. Builder should not keep references. Otherwise, caller has to be very careful
		// and will have to make copies of keys every time they add to builder, which is even worse.
		b.blockBaseKey = append(b.blockBaseKey[:0], key...)
		diffKey = key
	} else {
		diffKey = b.keyDiff(key)
	}

	h := header{
		baseLen: uint16(len(key) - len(diffKey)),
		diffLen: uint16(len(diffKey)),
	}
	b.buf = append(b.buf, h.Encode()...)
	b.buf = append(b.buf, diffKey...) // We only need to store the key difference.
	b.buf = v.EncodeTo(b.buf)
	b.entryEndOffsets = append(b.entryEndOffsets, uint32(b.writtenLen+len(b.buf))-b.blockBaseOffset)
	b.counter++ // Increment number of keys added for this current block.
}

func (b *Builder) finishBlock() error {
	b.buf = append(b.buf, u32SliceToBytes(b.entryEndOffsets)...)
	b.buf = append(b.buf, u32ToBytes(uint32(len(b.entryEndOffsets)))...)
	b.blockEndOffsets = append(b.blockEndOffsets, uint32(b.writtenLen+len(b.buf)))

	// Add base key.
	b.baseKeysBuf = append(b.baseKeysBuf, b.blockBaseKey...)
	b.baseKeysEndOffs = append(b.baseKeysEndOffs, uint32(len(b.baseKeysBuf)))

	// Reset the block for the next build.
	b.entryEndOffsets = b.entryEndOffsets[:0]
	b.counter = 0
	b.blockBaseKey = b.blockBaseKey[:0]
	b.blockBaseOffset = uint32(b.writtenLen + len(b.buf))
	b.writtenLen += len(b.buf)
	if err := b.w.Append(b.buf); err != nil {
		return err
	}
	b.buf = b.buf[:0]
	return nil
}

// Add adds a key-value pair to the block.
// If doNotRestart is true, we will not restart even if b.counter >= restartInterval.
func (b *Builder) Add(key []byte, value y.ValueStruct) error {
	if b.counter >= restartInterval {
		if err := b.finishBlock(); err != nil {
			return err
		}
	}
	b.addHelper(key, value)
	return nil // Currently, there is no meaningful error.
}

// ReachedCapacity returns true if we... roughly (?) reached capacity?
func (b *Builder) ReachedCapacity(capacity int64) bool {
	estimateSz := b.writtenLen + len(b.buf) +
		4*len(b.blockEndOffsets) +
		len(b.baseKeysBuf) +
		4*len(b.baseKeysEndOffs)
	return int64(estimateSz) > capacity
}

// EstimateSize returns the size of the SST to build.
func (b *Builder) EstimateSize() int {
	size := b.writtenLen + len(b.buf) + 4*len(b.blockEndOffsets) + len(b.baseKeysBuf) + 4*len(b.baseKeysEndOffs)
	if b.opt.EnableHashIndex {
		size += 3 * int(float32(len(b.hashEntries))/b.opt.HashUtilRatio)
	}
	return size
}

// Finish finishes the table by appending the index.
func (b *Builder) Finish() error {
	b.finishBlock() // This will never start a new block.
	b.buf = append(b.buf, u32SliceToBytes(b.blockEndOffsets)...)
	b.buf = append(b.buf, b.baseKeysBuf...)
	b.buf = append(b.buf, u32SliceToBytes(b.baseKeysEndOffs)...)
	b.buf = append(b.buf, u32ToBytes(uint32(len(b.baseKeysEndOffs)))...)

	// Write bloom filter.
	bloomFilter := bbloom.New(float64(len(b.hashEntries)), b.bloomFpr)
	for _, he := range b.hashEntries {
		bloomFilter.Add(he.hash)
	}
	bfData := bloomFilter.BinaryMarshal()
	b.buf = append(b.buf, bfData...)
	b.buf = append(b.buf, u32ToBytes(uint32(len(bfData)))...)

	if b.opt.EnableHashIndex {
		b.buf = buildHashIndex(b.buf, b.hashEntries, b.opt.HashUtilRatio)
	} else {
		b.buf = append(b.buf, u32ToBytes(0)...)
	}
	if err := b.w.Append(b.buf); err != nil {
		return err
	}
	ts := uint64(math.MaxUint64)
	if b.isExternal {
		// External builder doesn't append ts to the keys, the output sst should has a non-MaxUint64 global ts.
		ts = math.MaxUint64 - 1
	}
	var tsBuf [8]byte
	binary.BigEndian.PutUint64(tsBuf[:], ts)
	if err := b.w.Append(tsBuf[:]); err != nil {
		return err
	}

	return b.w.Finish()
}

func u32ToBytes(v uint32) []byte {
	var uBuf [4]byte
	binary.LittleEndian.PutUint32(uBuf[:], v)
	return uBuf[:]
}

func u32SliceToBytes(u32s []uint32) []byte {
	if len(u32s) == 0 {
		return nil
	}
	var b []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	hdr.Len = len(u32s) * 4
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&u32s[0]))
	return b
}

func bytesToU32Slice(b []byte) []uint32 {
	if len(b) == 0 {
		return nil
	}
	var u32s []uint32
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&u32s))
	hdr.Len = len(b) / 4
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&b[0]))
	return u32s
}

func bytesToU32(b []byte) uint32 {
	return binary.LittleEndian.Uint32(b)
}
