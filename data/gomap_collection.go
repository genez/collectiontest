package data

import "collectiontest/serialization"

type GoMap struct {
	data map[string]*serialization.Item
}

func NewGoMap() *GoMap {
	return &GoMap{
		data: map[string]*serialization.Item{},
	}
}

func (gm GoMap) Insert(key string, value *serialization.Item) {
	gm.data[key]=value
}

func (gm GoMap) Get(key string) *serialization.Item {
	return gm.data[key]
}