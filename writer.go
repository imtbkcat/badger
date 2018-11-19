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

package badger

import (
	"github.com/coocood/badger/y"
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"sync"
	"time"
)

const numLSMWorker = 5

type writeWorker struct {
	*DB
	writeLSMWorkers [numLSMWorker]*writeLSMWorker
	writeLSMBarrier sync.WaitGroup
	writeLSMCh      chan []*request
}

type writeLSMWorker struct {
	reqs   []*request
	wakeUp chan struct{}
}

func newWriteLSMWorker() *writeLSMWorker {
	return &writeLSMWorker{
		reqs:   make([]*request, 0, 128),
		wakeUp: make(chan struct{}, 1),
	}
}

func startWriteWorker(db *DB) *y.Closer {
	closer := y.NewCloser(1 + numLSMWorker)
	w := &writeWorker{
		DB:         db,
		writeLSMCh: make(chan []*request, 1),
	}
	go w.runWriteVLog(closer)
	go w.dispatchLSMWrites()
	for i := range w.writeLSMWorkers {
		w.writeLSMWorkers[i] = newWriteLSMWorker()
		go w.runWriteLSM(closer, i)
	}
	return closer
}

func (w *writeWorker) runWriteVLog(lc *y.Closer) {
	defer lc.Done()
	for {
		var r *request
		select {
		case r = <-w.writeCh:
		case <-lc.HasBeenClosed():
			w.closeWriteVLog()
			return
		}
		l := len(w.writeCh)
		reqs := make([]*request, l+1)
		reqs[0] = r
		for i := 0; i < l; i++ {
			reqs[i+1] = <-w.writeCh
		}
		err := w.vlog.write(reqs)
		if err != nil {
			w.done(reqs, err)
			return
		}
		for i := len(reqs) - 1; i >= 0; i-- {
			req := reqs[i]
			if w.updateOffset(req.Ptrs) {
				break
			}
		}
		w.writeLSMCh <- reqs
	}
}

func (w *writeWorker) runWriteLSM(lc *y.Closer, idx int) {
	defer lc.Done()
	ctx := w.writeLSMWorkers[idx]
	for range ctx.wakeUp {
		if len(ctx.reqs) == 0 {
			continue
		}
		w.writeLSM(ctx.reqs)
		ctx.reqs = make([]*request, 0, len(ctx.reqs))
		w.writeLSMBarrier.Done()
	}
}

func (w *writeWorker) closeWriteVLog() {
	close(w.writeCh)
	var reqs []*request
	for r := range w.writeCh { // Flush the channel.
		reqs = append(reqs, r)
	}
	err := w.vlog.write(reqs)
	if err != nil {
		w.done(reqs, err)
	} else {
		w.writeLSMCh <- reqs
	}
	close(w.writeLSMCh)
}

func (w *writeWorker) sendToLSMWriter(reqs []*request) error {
	var (
		size, count int64
		curr        int
		used        [numLSMWorker]bool
	)

	for curr < len(reqs) {
		req := reqs[curr]
		size += req.Size
		count += req.Count
		if count < w.opt.maxBatchCount && size < w.opt.maxBatchSize {
			idx := req.Fp % numLSMWorker
			lw := w.writeLSMWorkers[idx]
			if !used[idx] {
				used[idx] = true
				w.writeLSMBarrier.Add(1)
			}
			lw.reqs = append(lw.reqs, req)

			curr++
			if curr != len(reqs) {
				continue
			}
		}
		size = 0
		count = 0

		for i := range used {
			if used[i] {
				w.writeLSMWorkers[i].wakeUp <- struct{}{}
				used[i] = false
			}
		}
		w.writeLSMBarrier.Wait()

		// We need wait or ongoing write done, and then alloc a new mem table
		var i uint64
		var err error
		for err = w.ensureRoomForWrite(); err == errNoRoom; err = w.ensureRoomForWrite() {
			i++
			if i%100 == 0 {
				log.Warnf("Making room for writes")
			}
			// We need to poll a bit because both hasRoomForWrite and the flusher need access to s.imm.
			// When flushChan is full and you are blocked there, and the flusher is trying to update s.imm,
			// you will get a deadlock.
			time.Sleep(10 * time.Millisecond)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *writeWorker) dispatchLSMWrites() {
	for reqs := range w.writeLSMCh {
		if err := w.sendToLSMWriter(reqs); err != nil {
			w.done(reqs, err)
			break
		}
	}
	w.closeWriteLSMWorkers()
}

func (w *writeWorker) closeWriteLSMWorkers() {
	for i := range w.writeLSMWorkers {
		close(w.writeLSMWorkers[i].wakeUp)
	}
}

// writeLSM is called serially by only one goroutine.
func (w *writeWorker) writeLSM(reqs []*request) {
	if len(reqs) == 0 {
		return
	}
	var count int
	for _, b := range reqs {
		if len(b.Entries) == 0 {
			continue
		}
		count += len(b.Entries)
		if err := w.writeToLSM(b); err != nil {
			w.done(reqs, err)
			return
		}
	}
	w.done(reqs, nil)
	//log.Debugf("%d entries written", count)
	return
}

func (w *writeWorker) done(reqs []*request, err error) {
	for _, r := range reqs {
		r.Err = err
		r.Wg.Done()
	}
	if err != nil {
		log.Warnf("ERROR in Badger::writeLSM: %v", err)
	}
}

func (w *writeWorker) writeToLSM(b *request) error {
	if len(b.Ptrs) != len(b.Entries) {
		return errors.Errorf("Ptrs and Entries don't match: %+v", b)
	}

	for i, entry := range b.Entries {
		if entry.meta&bitFinTxn != 0 {
			continue
		}
		if w.shouldWriteValueToLSM(entry) { // Will include deletion / tombstone case.
			w.mt.Put(entry.Key,
				y.ValueStruct{
					Value:    entry.Value,
					Meta:     entry.meta,
					UserMeta: entry.UserMeta,
				})
		} else {
			var offsetBuf [vptrSize]byte
			w.mt.Put(entry.Key,
				y.ValueStruct{
					Value:    b.Ptrs[i].Encode(offsetBuf[:]),
					Meta:     entry.meta | bitValuePointer,
					UserMeta: entry.UserMeta,
				})
		}
	}
	return nil
}
