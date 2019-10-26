package cache

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCacheMgr(t *testing.T) {
	cacheMgr := &CacheManagerImpl{
		tmp: make(map[string]CacheEntry),
	}
	assert.NoError(t, cacheMgr.Add("1", CacheEntryImpl{
		id: "1",
	}))
	assert.Error(t, cacheMgr.Add("1", CacheEntryImpl{
		id: "1",
	}))

	assert.NoError(t, cacheMgr.Pin("1"))
	assert.Error(t, cacheMgr.Pin("1"))
	assert.NoError(t, cacheMgr.Release("1"))
	assert.Error(t, cacheMgr.Release("1"))
	assert.NoError(t, cacheMgr.Free("1"))
	assert.Error(t, cacheMgr.Free("1"))
}
