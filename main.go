package main

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/google/uuid"
	bolt "go.etcd.io/bbolt"
)

const BUCKET_NAME = "b1"
const TEST_KEY_1 = "afffe217-9890-4bb8-adf3-d025700fdb3e"
const TEST_KEY_2 = "dfffe217-9890-4bb8-adf3-d025700fdb3e"
const TEST_KEY_3 = "ffffe217-9890-4bb8-adf3-d025700fdb3e"

func AssertBucket(db *bolt.DB) {
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(BUCKET_NAME))
		return err
	})
	if err != nil {
		log.Fatalf("CreateBucketIfNotExists(): %+v\n", err)
	}
}

func Add50k(db *bolt.DB) {
	start := time.Now()
	err := db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(BUCKET_NAME))

		for i := 0; i < 1000*50; i++ {
			bucket.Put([]byte(uuid.New().String()), []byte("1"))
		}

		// make sure two test keys always exist, but skip the last one
		bucket.Put([]byte(TEST_KEY_1), []byte("1"))
		bucket.Put([]byte(TEST_KEY_2), []byte("1"))

		return nil
	})
	if err != nil {
		log.Fatalf("Add100k(): %+v\n", err)
	}
	fmt.Printf("Adding 50k keys took %+v\n", time.Since(start))
}

func CountKeys(db *bolt.DB) {
	start := time.Now()
	db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(BUCKET_NAME))
		c := bucket.Cursor()
		ctr := 0
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			ctr++
		}
		fmt.Printf("Found %+v keys\n", ctr)
		return nil
	})
	fmt.Printf("Counting took %+v\n", time.Since(start))
}

func FindKey(db *bolt.DB, key string) (val string) {
	start := time.Now()
	db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(BUCKET_NAME))
		v := bucket.Get([]byte(key))
		if v != nil {
			val = string(v)
		}
		return nil
	})
	fmt.Printf("Find key took %+v\n", time.Since(start))
	return
}

func main() {
	var err error

	findKey := false
	countKeys := false
	add50k := false

	for _, a := range os.Args {
		if a == "f" {
			findKey = true
		} else if a == "c" {
			countKeys = true
		} else if a == "a" {
			add50k = true
		}
	}

	start := time.Now()
	db, err := bolt.Open("./db-1.db", 0666, nil)
	if err != nil {
		log.Fatalf("Open(): %+v\n", err)
	}
	fmt.Printf("Open() took %+v\n", time.Since(start))

	AssertBucket(db)

	if findKey {
		fmt.Printf("Finding key %s => %+v\n", TEST_KEY_3, FindKey(db, TEST_KEY_3))
		fmt.Printf("Finding key %s => %+v\n", TEST_KEY_1, FindKey(db, TEST_KEY_1))
		fmt.Printf("Finding key %s => %+v\n", TEST_KEY_2, FindKey(db, TEST_KEY_2))
	}

	if countKeys {
		CountKeys(db)
	}

	if add50k {
		fmt.Printf("Adding 50k keys...\n")
		Add50k(db)
	}

	start = time.Now()
	db.Close()
	fmt.Printf("Close() took %+v\n", time.Since(start))

	PrintMemUsage()

	fmt.Printf("Bye! %v\n", 111)
}

func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
