package main

import (
	"fmt"

	"github.com/theakula/pandora_db"
)

func main() {
	db := pandora_db.KV{}
	db.Path = "test.db"
	
	err := db.Open()
	if err != nil {
		fmt.Println("failed to open db: ", err)
		return
	}
	defer db.Close()
	
	db.Set([]byte("dog1"), []byte("qwe"))
	db.Set([]byte("dog2"), []byte("req"))

	val, ok := db.Get([]byte("dog1"))
	if !ok {
		fmt.Println("failed to get value: ", err)
	}

	fmt.Println(string(val))

	val, ok = db.Get([]byte("dog2"))
	if !ok {
		fmt.Println("failed to get value: ", err)
	}

	fmt.Println(string(val))
}