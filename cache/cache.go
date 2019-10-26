package cache

import (
	"fmt"
	"github.com/pingcap/errors"
	"io/ioutil"
	"strings"
	"sync"
)

type CacheEntry interface {
	// cache file id
	CacheID() string
	// deallocate entry
	Deallocate() error
	// init entry
	Init() error
	// is file pinned
	Pinned() bool
	// pin file
	Pin() error
	// unpin file
	Unpin() error
	// file size
	CacheSize() int
	// file in local ssd
	IsInLocal() bool
	// SetInLocal
	SetInLocal(inLocal bool) bool
}

type CacheManager interface {
	// scan uploading files
	Open() error
	// Add file to cache
	Add(id string, entry *CacheEntry, upload bool) error
	// File with ID is no longer needed, can be deleted from S3 and local.
	Free(id string) error
	// This file is needed, if not stay in local cache fetch from S3 and call `CacheEntry.Init`
	Pin(id string) error
	// Call `CacheEntry.Deallocate` to release entry, then it is safe from cache manager to remove cache of this file.
	Release(id string) error
}

type CacheEntryImpl struct{
	id string
	pinned int
	inLocal bool
}

func (entry CacheEntryImpl) IsInLocal() bool {
	return entry.inLocal
}

func (entry CacheEntryImpl) SetInLocal(inLocal bool) {
	entry.inLocal = inLocal

}

func (entry CacheEntryImpl) CacheID() string {
	return entry.id
}

func (entry CacheEntryImpl) Pinned() bool {
	return entry.pinned > 0
}

func (entry CacheEntryImpl) Pin() error {
	entry.pinned++
	return nil
}

func (entry CacheEntryImpl) Unpin() error {
	if entry.pinned > 0 {
		entry.pinned--
		return nil
	}
	return errors.Errorf("entry not pinned")
}

func (entry CacheEntryImpl) Deallocate() error {
	fmt.Println("cache entry deallocate")
	return nil
}

func (entry CacheEntryImpl) Init() error {
	fmt.Println("cache entry init")
	return nil
}

type CacheManagerImpl struct{
	// file dir, fileDir + id = filePath
	fileDir string
	// max files in cache manager
	maxSize int

	mu sync.Mutex
	// lru cache
	cache *LRU
	// current size
	curSize int
}

func NewCacheManager(filePath string, maxFiles int) *CacheManager {
	return nil
}

func (mgr *CacheManagerImpl) getFileName(id string) string {
	return fmt.Sprintf("%s/%s", mgr.fileDir, id)
}

func (mgr *CacheManagerImpl) getUploadingFileName(id string) string {
	return fmt.Sprintf("%s/%s.uploading", mgr.fileDir, id)
}

func (mgr *CacheManagerImpl) getEntry(id string) (CacheEntry, error) {
	value, ok := mgr.cache.Peek(id)
	if !ok {
		return nil, errors.Errorf("%s not exist", id)
	}
	entry := value.(CacheEntry)
	return entry, nil
}

func (mgr *CacheManagerImpl) ensureFileSize(newSize int) error {
	for mgr.curSize + newSize > mgr.maxSize {
		_, value, ok := mgr.cache.GetOldestCanEvict()
		if !ok {
			return errors.Errorf("cache full")
		}
		entry := value.(CacheEntry)
		if !entry.IsInLocal() {
			panic("unexpected error: %d can evict but not in local")
		}
		mgr.curSize -= entry.CacheSize()
		mgr.removeLocalFile(entry.CacheID())
	}
	return nil
}

func (mgr *CacheManagerImpl) uploadRemoteFile(id string) error {
	return nil
}

func (mgr *CacheManagerImpl) downloadRemoteFile(id string) error {
	entry, err:= mgr.getEntry(id)
	if err != nil {
		return err
	}
	err = mgr.ensureFileSize(entry.CacheSize())
	if err != nil {
		return err
	}
	// download
	return nil
}

func (mgr *CacheManagerImpl) removeRemoteFile(id string) error {
	return nil
}

func (mgr *CacheManagerImpl) removeLocalFile(id string) error {
	entry, err := mgr.getEntry(id)
	if err != nil {
		return err
	}
	// TODO remove file
	mgr.curSize -= entry.CacheSize()
	entry.SetInLocal(false)
	return nil
}

func (mgr *CacheManagerImpl) Open() error {
	fileInfos, err := ioutil.ReadDir(mgr.fileDir)
	if err != nil {
		return errors.Wrapf(err, "Error while opening cache uploading files")
	}
	for _, fileInfo := range fileInfos {
		if !strings.HasSuffix(fileInfo.Name(), ".uploading") {
			continue
		}
		fsz := len(fileInfo.Name())
		origFileId := fileInfo.Name()[:fsz-10]
		err := mgr.uploadRemoteFile(origFileId)
		if err != nil {
			return err
		}
	}
	return nil
}

func (mgr *CacheManagerImpl) Add(id string, entry CacheEntry, upload bool) error {
	if upload {
		if err := mgr.uploadRemoteFile(id); err != nil {
			return err
		}
	}
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	mgr.cache.Add(id, entry)
	return nil
}

func (mgr *CacheManagerImpl) Free(id string) error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	value, ok := mgr.cache.Peek(id)
	if !ok {
		return errors.Errorf("%s not exist in cache list", id)
	}
	oldEntry := value.(CacheEntry)
	if oldEntry.Pinned() {
		return errors.Errorf("%s is pinned", id)
	}
	err := mgr.removeRemoteFile(id)
	if err != nil {
		return err
	}
	err = mgr.removeLocalFile(id)
	if err != nil {
		return err
	}
	mgr.cache.Remove(id)
	return nil
}

func (mgr *CacheManagerImpl) Pin(id string) error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	value, ok := mgr.cache.Get(id)
	if !ok {
		return errors.Errorf("%s not in cache list", id)
	}
	entry := value.(CacheEntry)
	err := mgr.downloadRemoteFile(id)
	if err != nil {
		return err
	}
	return entry.Pin()
}

func (mgr *CacheManagerImpl) Release(id string) error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	value, ok := mgr.cache.Peek(id)
	if !ok {
		return errors.Errorf("%s not in cache list", id)
	}
	entry := value.(CacheEntry)
	return entry.Unpin()
}

