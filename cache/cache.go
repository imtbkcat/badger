package cache

import (
	"fmt"
	"github.com/pingcap/errors"
)

type CacheEntry interface {
	CacheID() string
	Deallocate() error
	Init() error
}

type CacheManager interface {
	// Add file to cache
	Add(id string, entry CacheEntry) error
	// File with ID is no longer needed, can be deleted from S3 and local.
	Free(id string) error
	// This file is needed, if not stay in local cache fetch from S3 and call `CacheEntry.Init`
	Pin(id string) error
	// Call `CacheEntry.Deallocate` to release entry, then it is safe from cache manager to remove cache of this file.
	Release(id string) error
}

type CacheEntryImpl struct{
	id string
}

func (entry CacheEntryImpl) CacheID() string {
	return entry.id
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
	tmp map[string]CacheEntry
	pined []string
	// lru list
}

func (mgr *CacheManagerImpl) Add(id string, entry CacheEntry) error {
	if _, ok := mgr.tmp[id]; ok {
		return errors.Errorf("%s already exist", id)
	}
	mgr.tmp[id] = entry
	return nil
}

func (mgr *CacheManagerImpl) Free(id string) error {
	if _, ok := mgr.tmp[id]; !ok {
		return errors.Errorf("%s not exist", id)
	}
	delete(mgr.tmp, id)
	for i := range mgr.pined {
		if mgr.pined[i] == id {
			mgr.pined = append(mgr.pined[0:i], mgr.pined[i+1:]...)
			return nil
		}
	}
	return nil
}

func (mgr *CacheManagerImpl) Pin(id string) error {
	if _, ok := mgr.tmp[id]; !ok {
		return errors.Errorf("%s not exist", id)
	}
	for i := range mgr.pined {
		if mgr.pined[i] == id {
			return errors.Errorf("%s already pinned", id)
		}
	}
	mgr.pined = append(mgr.pined, id)
	return nil
}

func (mgr *CacheManagerImpl) Release(id string) error {
	if _, ok := mgr.tmp[id]; !ok {
		return errors.Errorf("%s not exist", id)
	}
	for i := range mgr.pined {
		if mgr.pined[i] == id {
			mgr.pined = append(mgr.pined[0:i], mgr.pined[i+1:]...)
			return nil
		}
	}
	return errors.Errorf("%s not pinned", id)
}

