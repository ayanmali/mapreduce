/*
MapReduce

Two functions:
Map - processes input pair and produces intermediate KV pairs
All intermediate values associated w/ same intermediate key and passes them to the Reduce function

Reduce - accepts an intermediate key and an iterator of all values for that key. These values are merged together to form an output.
*/
package main

import (
	"fmt"
	"strings"
)

type Pair struct {
	first  any
	second any
}

var intermediateBuffer = make([]Pair, 0)

func Map(key string, value string) {
	for _, word := range strings.Split(value, " ") {
		// emit intermediate KV pair
		MapEmit(word, 1)
	}
	//return key, value
}

func MapEmit[K, V any](intermediateKey K, intermediateValue V) {
	intermediateBuffer = append(intermediateBuffer, Pair{intermediateKey, intermediateValue})
}

func GroupByKey() map[any][]any {
	grouped := make(map[any][]any)
	for _, pair := range intermediateBuffer {
		_, present := grouped[pair.first]
		if present {
			grouped[pair.first] = append(grouped[pair.first], pair.second)
		} else {
			grouped[pair.first] = []any{pair.second}
		}
	}
	return grouped

}

// func Reduce[K, V any](key K, values []V) any {
// 	return values[0]
// }

func main() {
	database := map[string]string{
		"file1": "apple banana cherry apple date elderberry",
		"file2": "fig grape orange grape grape fig",
		"file3": "grape apple banana banana banana banana banana",
	}

	for filename, document := range database {
		Map(filename, document)
	}
	fmt.Println("Intermediate buffer:")
	fmt.Println(intermediateBuffer)
	fmt.Println("Grouped:")
	grouped := GroupByKey()
	fmt.Println(grouped)

}
