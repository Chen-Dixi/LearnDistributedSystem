package main

import "time"
import "fmt"

type Test struct {
	timer *time.Timer
}
	
	
func main() {
	test_data := &Test{
		timer : time.NewTimer(1 * time.Second),
	}
	
	for {
		<- test_data.timer.C
		fmt.Println("tic...toc")
		test_data.timer.Reset(1 * time.Second)
	}
}