package data

import (
	"collectiontest/serialization"
	"github.com/akrylysov/pogreb"
	"github.com/akrylysov/pogreb/fs"
	"log"
)


type PogrebMap struct {
	data *pogreb.DB
}


func NewPogrebMap() *PogrebMap {
	db, err := pogreb.Open("data.pogreb", &pogreb.Options{
		BackgroundSyncInterval: 0,
		FileSystem:             fs.OS,
	})
	if err != nil {
		log.Fatal(err)
	}

	return &PogrebMap{
		data: db,
	}
}


func (pg *PogrebMap) Insert(key string, value *serialization.Item) {
	buff, err := value.MarshalBinary()
	if err != nil {
		log.Fatal(err)
	}
	err = pg.data.Put([]byte(key), buff)
	if err != nil {
		log.Fatal(err)
	}
}

func (pg *PogrebMap) Get(key string) *serialization.Item {
	item := &serialization.Item{}
	buff, err := pg.data.Get([]byte(key))
	if err != nil {
		log.Fatal(err)
	}
	err = item.UnmarshalBinary(buff)
	if err != nil {
		log.Fatal(err)
	}
	return item
}
