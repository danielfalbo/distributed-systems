package main

//
// Simple sequential MapReduce.
//
// 	>_ cd src/main
// 	>_ go build -buildmode=plugin ../mrapps/wc.go
// 	>_ rm mr-out*
// 	>_ go run mrsequential.go wc.so pg*.txt
// 	>_ more mr-out-0
//

import "fmt"
import "6.5840/mr"
import "plugin"
import "os"
import "log"
import "io/ioutil"
import "sort"

/* For sorting by key. */

// Type.
//
// As this type is identical to []mr.KeyValue,
// you can cast back and forth between []mr.KeyValue and ByKey freely:
// they are identical in memory.

type ByKey []mr.KeyValue

// Methods required for custom sorting.
//
// By writing 'func (a ByKey) ...' we're attaching these functions
// to 'ByKey' so that for any 'a' of type 'ByKey', we will be able
// to use our functions as 'a.Func(...)'.
//
// Implementing 'Len', 'Swap', and 'Less' makes our type satisfy the
// 'sort.Interface' interface, so that later we will be able to call
// 'sort.Sort(ByKey(data))' for any 'data' array of type '[]mr.KeyValue'.

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

/* Main */

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: mrsequential xxx.so inputfiles...\n")
		os.Exit(1)
	}

	mapf, reducef := loadPlugin(os.Args[1])

	//
	// Read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	//
	intermediate := []mr.KeyValue{}
	for _, filename := range os.Args[2:] {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}

	//
	// A big difference from real MapReduce is that all the
	// intermediate data is in one place, intermediate[],
	// rather than being partitioned into NxM buckets.
	//

	sort.Sort(ByKey(intermediate))

	oname := "mr-out-0"
	ofile, _ := os.Create(oname)

	//
	// Call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
}

// Load the application Map and Reduce functions
// from a plugin file, e.g. '../mrapps/wc.so'.
//
// This function takes a plugin 'filename' as argument and returns two other
// functions: the 'Map' and 'Reduce' implementations from the given 'filename'.
//
// Map and Reduce take strings because they deal with entire raw file content.
// See, for example, 'mrapps/wc.go' to understand the signature and behaviour
// of 'Map' and 'Reduce'.
func loadPlugin(filename string) (func(string, string) []mr.KeyValue,
																	func(string, []string) string) {
	// Load plugin file.
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}

	// Extract 'Map'.
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue)

	// Extract 'Reduce'.
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	// Return 'Map' and 'Reduce'.
	return mapf, reducef
}
