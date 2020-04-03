package data

import (
	"collectiontest/serialization"
	"github.com/allegro/bigcache"
	"log"
)


type BigCacheMap struct {
	data *bigcache.BigCache
}

func NewBigCacheMap() *BigCacheMap {
	config := bigcache.Config {
		// number of shards (must be a power of 2)
		Shards: 2048,

		// rps * lifeWindow, used only in initial memory allocation
		MaxEntriesInWindow: 1000 * 10 * 60,

		// max entry size in bytes, used only in initial memory allocation
		MaxEntrySize: 1000,

		// prints information about additional memory allocation
		Verbose: false,

		// cache will not allocate more memory than this limit, value in MB
		// if value is reached then the oldest entries can be overridden for the new ones
		// 0 value means no size limit
		HardMaxCacheSize: 0,

		// callback fired when the oldest entry is removed because of its expiration time or no space left
		// for the new entry, or because delete was called. A bitmask representing the reason will be returned.
		// Default value is nil which means no callback and it prevents from unwrapping the oldest entry.
		OnRemove: func(key string, entry []byte) {
			log.Println("OnRemove", key)
		},

		// OnRemoveWithReason is a callback fired when the oldest entry is removed because of its expiration time or no space left
		// for the new entry, or because delete was called. A constant representing the reason will be passed through.
		// Default value is nil which means no callback and it prevents from unwrapping the oldest entry.
		// Ignored if OnRemove is specified.
		OnRemoveWithReason: func(key string, entry []byte, reason bigcache.RemoveReason) {
			log.Println("OnRemoveWithReason", key, reason)
		},
	}

	cache, initErr := bigcache.NewBigCache(config)
	if initErr != nil {
		log.Fatal(initErr)
	}

	return &BigCacheMap{
		data: cache,
	}
}

func (bcm BigCacheMap) Insert(key string, value *serialization.Item) {
	buff, err := value.MarshalBinary()
	if err != nil {
		log.Fatal(err)
	}
	err = bcm.data.Set(key, buff)
	if err != nil {
		log.Fatal(err)
	}
}

func (bcm BigCacheMap) Get(key string) *serialization.Item {
	item := &serialization.Item{}
	buff, err := bcm.data.Get(key)
	if err != nil {
		log.Fatal(err)
	}
	err = item.UnmarshalBinary(buff)
	if err != nil {
		log.Fatal(err)
	}
	return item
}
