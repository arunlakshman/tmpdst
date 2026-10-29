package a

import (
	"errors"
	"sort"
	"time" // want `import "time" is not on the deterministic allow-list`
)

var ErrBad = errors.New("bad")

var _ = sort.Ints

var counter int // want `package-level variable counter`

var cache = map[string]int{} //detcheck:ok read-only lookup table, never written

func Bad(m map[string]int, ch chan int) { // want `channel type`
	go func() {}()     // want `go statement`
	ch <- 1            // want `channel send`
	<-ch               // want `channel receive`
	select {}          // want `select statement`
	for k := range m { // want `range over map`
		_ = k
	}
	_ = time.Now()
}

func Good(m map[string]int) []string {
	keys := make([]string, 0, len(m))
	//detcheck:ok collecting keys to sort; order does not escape
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
