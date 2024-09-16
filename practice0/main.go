package main

import (
	"fmt"
	// "math/rand"
	"time"
)

func main() {
	b := [100]byte{}
	go func() {
		for {
			filler(b[:50], '0', '1')
			time.Sleep(time.Second)
		}
	}()

	go func() {
		for {
			filler(b[50:], 'X', 'Y')
			time.Sleep(time.Second)
		}
	}()

	go func() {
		for {
			fmt.Println(string(b[:]))
			time.Sleep(time.Second)
		}
	}()

	select {}
}

func filler(b []byte, ifzero byte, ifnot byte) {
	for i := 0; i < len(b); i++ {
		if i%2 == 0 {
			b[i] = ifzero
		} else {
			b[i] = ifnot
		}
		//или так для рандома :) b[i] = map[bool]byte{true: ifzero, false: ifnot}[rand.Intn(2) == 0]
	}
}
