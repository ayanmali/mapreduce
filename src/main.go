/*
MapReduce

Two functions:
Map - processes input pair and produces intermediate KV pairs
All intermediate values associated w/ same intermediate key and passes them to the Reduce function

Reduce - accepts an intermediate key and an iterator of all values for that key. These values are merged together to form an output.
*/
package mapreduce

import (
	"fmt"
	"log"
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
}

func MapEmit[K, V any](intermediateKey K, intermediateValue V) {
	intermediateBuffer = append(intermediateBuffer, Pair{intermediateKey, intermediateValue})
}

func groupByKey() map[any][]any {
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

func Reduce(key any, iter []any) {
	count := 0
	for _, value := range iter {
		i, ok := value.(int)
		if !ok {
			log.Fatalf("Type assertion failed: value is not an int")
		}
		count += i
	}
	ReduceEmit(key, count)
}

func ReduceEmit(key any, result any) {
	fmt.Println(key, result)
}

func main() {
	database := map[string]string{
		"file1": "apple banana cherry apple date elderberry",
		"file2": "fig grape orange grape grape fig",
		"file3": "grape apple banana banana banana banana banana",
	}

	for k, v := range database {
		Map(k, v)
	}
	fmt.Println("Intermediate buffer:")
	fmt.Println(intermediateBuffer)
	fmt.Println("Grouped:")
	grouped := groupByKey()
	fmt.Println(grouped)
	for key, iter := range grouped {
		Reduce(key, iter)
	}

}
