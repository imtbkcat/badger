package cache

type CacheEntry interface {
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
