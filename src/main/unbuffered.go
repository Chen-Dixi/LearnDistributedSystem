package main

import (
	"time"
	"fmt"
)

func main() {
	c := make(chan bool)
	go func() {
		time.Sleep(1 * time.Second)
		<-c
	}()
	start := time.Now()
	c <- true
	fmt.Printf("send took %v\n", time.Since(start))
}