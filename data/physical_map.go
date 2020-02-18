package data

import (
	"collectiontest/serialization"
	"fmt"
	"sync"
)

type PhysicalMap struct {
	size int
	db *GoMap
}

func (pm *PhysicalMap) Close() {
}

func (pm *PhysicalMap) Get(primaryKey string) *serialization.Item {
	item := pm.db.Get(primaryKey)
	return item
}

func (pm *PhysicalMap) BulkInsert(wg *sync.WaitGroup, items <- chan *serialization.Item) error {
	defer wg.Done()
	for item := range items {
		pm.db.Insert(fmt.Sprintf("%d%s", item.NtinId, item.Serial), item)
		pm.size++
	}
	return nil
}

func (pm *PhysicalMap) Insert(item *serialization.Item) {
	pm.db.Insert(fmt.Sprintf("%d%s", item.NtinId, item.Serial), item)
	pm.size++
}

func (pm *PhysicalMap) Update(item *serialization.Item) {
	pm.db.Insert(fmt.Sprintf("%d%s", item.NtinId, item.Serial), item)
}

func (pm *PhysicalMap) Len() int {
	return pm.size
}

func NewPhysicalMap() *PhysicalMap {
	pm := PhysicalMap{}
	pm.db = NewGoMap()
	return &pm
}


