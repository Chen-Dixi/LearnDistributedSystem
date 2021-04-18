package main

import (
	"time"
	"sync"
	"math/rand"
)

func main(){
	count := 0
	finished := 0
	var mu sync.Mutex
	cond := sync.NewCond(&mu)

	for i := 0; i < 10; i++ {
		go func(){
			vote := RequestVote()
			mu.Lock()
			defer mu.Unlock()
			if vote{
				count++
			}
			finished++
			// the data is changed, then we call cond.Broadcast()
			cond.Broadcast()
		}()
	}

	mu.Lock()
	for count < 5 && finished != 10 {
		cond.Wait()
	}
	if count >= 5 {
		println("received 5+ votes")
	}else{
		print("lost")
	}
	mu.Unlock()
}

func RequestVote() bool {
	time.Sleep(time.Duration(rand.Intn(100))*time.Millisecond)
	return rand.Int() % 2 == 0
}