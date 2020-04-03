package data

import (
	"collectiontest/serialization"
	"github.com/cornelk/hashmap"
)

type CornelkHashmap struct {
	data *hashmap.HashMap
}

func NewCornelkHashmap() *CornelkHashmap {
	return &CornelkHashmap{
		data: hashmap.New(4 * 1000 * 1000),
	}
}

func (hm CornelkHashmap) Insert(key string, value *serialization.Item) {
	hm.data.Insert(key, value)
}

func (hm CornelkHashmap) Get(key string) *serialization.Item {
	val, ok := hm.data.GetStringKey(key)
	if !ok {
		return nil
	}
	return val.(*serialization.Item)
}


