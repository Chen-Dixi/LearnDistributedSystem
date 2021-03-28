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

	var arr1 [5]int
	var arr2 [5]int = [5]int{1,2,3,4,5}
	arr1 = arr2
	arr1[2] = 0
	fmt.Print(arr2)

	fmt.Print(arr1)
}