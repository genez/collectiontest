package data

import (
	"collectiontest/serialization"
	"encoding/binary"
	"unsafe"
)

/*
#include "hashdict.h"
*/
import "C"


type HashDict struct {
	dic *C.struct_dictionary
	dataBuff []byte
}

func NewHashDict() *HashDict {
	return &HashDict{
		dic: C.dic_new(C.int(10_000_000)),
		dataBuff:make([]byte, 2048),
	}
}

func (hd *HashDict) Close() error {
	C.free(unsafe.Pointer(hd.dic))
	return nil
}

func (hd *HashDict) Insert(key string, value *serialization.Item) {
	ckey := C.CString(key)
	defer C.free(unsafe.Pointer(ckey))

	l, err := value.MarshalLen()
	if err != nil {
		panic(err)
	}

	lenSize := binary.Size(uint32(l))

	binary.LittleEndian.PutUint32(hd.dataBuff, uint32(l))
	marshalSize := value.MarshalTo(hd.dataBuff[lenSize:])
	if err != nil {
		panic(err)
	}

	//DO NOT FREE this cdata area
	cdata := C.CBytes(hd.dataBuff[:lenSize+marshalSize])

	var alreadyPresent, err1 = C.dic_add((*C.struct_dictionary)(hd.dic), unsafe.Pointer(ckey), C.int(len(key)))
	if err1 != nil {
		panic(err1)
	}

	//must eventually deallocate previous byte array
	if int(alreadyPresent) == 1 {
		C.free(*(hd.dic.value))
	}

	*(hd.dic.value) = unsafe.Pointer(cdata)
}

func (hd *HashDict) Get(key string) *serialization.Item {
	ckey := C.CString(key)
	defer C.free(unsafe.Pointer(ckey))

	var found, err = C.dic_find((*C.struct_dictionary)(hd.dic), unsafe.Pointer(ckey), C.int(len(key)))
	if err != nil {
		panic(err)
	}
	if int(found) == 0 {
		return nil
	}

	var l uint32 = 0

	lenSize := binary.Size(l)

	lenBuff := C.GoBytes(*hd.dic.value, C.int(lenSize))
	l = binary.LittleEndian.Uint32(lenBuff)

	buff := C.GoBytes(*hd.dic.value, C.int(lenSize+int(l)))

	item := serialization.Item{}
	_, err1 := item.Unmarshal(buff[lenSize:])
	if err != nil {
		panic(err1)
	}
	//fmt.Println(key, n)

	return &item
}

//https://github.com/exebook/hashdict.c

