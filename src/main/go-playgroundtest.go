package main

import (
	"fmt"
)

type Test struct{
	value int
}
func main() {
	
	o1 := Test{
		value: 1,
	}
	o2 := o1
	o2.value = 2

	fmt.Print(o1.value)
}