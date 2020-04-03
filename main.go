package main

import (
	"collectiontest/data"
	"collectiontest/serialization"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"golang.org/x/crypto/blake2b"
	"log"
	"os"
	"runtime/pprof"
	"runtime/trace"
	"time"
)

func main() {

	f, err := os.Create("trace.out")
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	dt := data.NewPhysicalMap()

	log.Println("Loading...")

	var item serialization.Item
	for i := 0; i < 6*1000_000; i++ {
		item.NtinId = 42
		item.Serial =       fmt.Sprintf("%015d", i)
		item.Status =       1
		item.ParentNtinId = 69
		item.ParentSerial = fmt.Sprintf("%015d", (20*1000_000)-i)
		item.Attributes = []*serialization.Attribute{
			{"Pippo",fmt.Sprintf("%X", i)},
		}
		dt.Insert(&item)
	}

	err = trace.Start(f)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("...SetCodeStatus")

	for i := 0; i < dt.Len(); i++ {
		serial := i % dt.Len()
		SetCodeStatus(ctx, dt,42, fmt.Sprintf("%015d", serial), 20)
	}

	log.Println("...done")

	trace.Stop()

	f2, err := os.Create("heap.out")
	defer f2.Close()

	pprof.WriteHeapProfile(f2)

	fmt.Print("finito")
}

func SetCodeStatus(ctx context.Context, dt *data.PhysicalMap, ntinId int, serial string, newStatus int32) {
	trace.WithRegion(ctx, "SetCodeStatus", func() {
		key := fmt.Sprintf("%d%s", ntinId, serial)
		item := dt.Get(key)

		js, err := json.Marshal(item)
		if err != nil {
			log.Fatal(err)
		}

		ts := time.Now().Format(time.RFC3339Nano)

		hash, err := blake2b.New256([]byte(ts))
		if err != nil {
			log.Fatal(err)
		}
		hc := hash.Sum(js)

		item.Status = newStatus
		item.HelperCode = base64.StdEncoding.EncodeToString(hc)
		item.Attributes = append(item.Attributes, &serialization.Attribute{
			Name:  "LastUpdate",
			Value: time.Now().Format(time.RFC3339Nano),
		})

		dt.Update(item)
	})
}