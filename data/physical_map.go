package data

import (
	"collectiontest/serialization"
	"fmt"
	"log"
	"sync"
)

type Collection interface {
	Get(key string) *serialization.Item
	Insert(key string, value *serialization.Item)
}

type PhysicalMap struct {
	size int
	db Collection
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
		k := fmt.Sprintf("%d%s", item.NtinId, item.Serial)
		pm.db.Insert(k, item)
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
	var err error
	pm.db = NewHashDict()
	if err != nil {
		log.Fatal(err)
	}
	return &pm
}


