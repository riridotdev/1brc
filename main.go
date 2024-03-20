package main

import (
	"bytes"
	"flag"
	"fmt"
	"math"
	"os"
	"runtime"
	"runtime/pprof"
	"slices"
	"syscall"
	"unsafe"
)

var cpuProfile = flag.Bool("profile", false, "output performance profile")

func main() {
	flag.Parse()

	if *cpuProfile {
		profile, err := os.Create("cpu.profile")
		if err != nil {
			panic(err)
		}
		defer profile.Close()

		if err := pprof.StartCPUProfile(profile); err != nil {
			panic(err)
		}
		defer pprof.StopCPUProfile()
	}

	if len(flag.Args()) != 1 {
		fmt.Printf("Usage: 1brc [input-file]\n")
		os.Exit(1)
	}

	inputFile := flag.Args()[0]

	file, err := os.Open(inputFile)
	if err != nil {
		fmt.Printf("Error reading file at %q: %s\n", inputFile, err)
		os.Exit(1)
	}
	defer file.Close()

	stat, err := os.Stat(inputFile)
	if err != nil {
		fmt.Printf("Error reading filestat for %q: %s\n", inputFile, err)
		os.Exit(1)
	}

	fileSize := int(stat.Size())

	fmap, err := syscall.Mmap(int(file.Fd()), 0, fileSize, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		fmt.Printf("Error mmapping file: %s\n", err)
		os.Exit(1)
	}
	/* defer syscall.Munmap(fmap) */

	numCpu := runtime.NumCPU()

	splits := make([]int, numCpu)
	for i := range splits {
		if i == 0 {
			continue
		}

		start := i * (fileSize / numCpu)

		for fmap[start-1] != '\n' {
			start += 1
		}

		splits[i] = start
	}

	done := make(chan hashMap)

	for i := range splits {
		go func(i int) {
			start := splits[i]
			end := fileSize

			if i != len(splits)-1 {
				end = splits[i+1]
			}

			done <- processRange(fmap[start:end])
		}(i)
	}

	results := newHashMap(16)
	for i := 0; i < numCpu; i++ {
		hm := <-done

		for i, occupied := range hm.occupied {
			if !occupied {
				continue
			}

			item := hm.data[i]

			result, ok := results.get(item.name)
			if !ok {
				result = newResult(item.name)
			}

			result = result.aggregate(item)

			results.set(result)
		}
	}

	sorted := []result{}
	for i, occupied := range results.occupied {
		if !occupied {
			continue
		}
		sorted = append(sorted, results.data[i])
	}

	slices.SortFunc(sorted, func(r1, r2 result) int {
		return bytes.Compare(r1.name.value, r2.name.value)
	})

	fmt.Print("{")
	for i, result := range sorted {
		if i != 0 {
			fmt.Print(", ")
		}
		fmt.Printf(
			"%s=%.1f/%.1f/%.1f",
			result.name.value,
			result.min,
			result.total/float64(result.count),
			result.max,
		)
	}
	fmt.Print("}")
}

func processRange(fmap []byte) hashMap {
	hm := newHashMap(16)

	idx := 0
	for idx != len(fmap) {
		n, name := parseName(fmap[idx:])
		idx += (n + 1)

		n, value := parseValue(fmap[idx:])
		idx += (n + 1)

		r, ok := hm.get(name)
		if !ok {
			r = newResult(name)
		}

		r = r.update(value)

		hm.set(r)
	}

	return hm
}

func parseValue(b []byte) (int, float64) {
	read := 0
	ptr := unsafe.Pointer(&b[0])

	sign := 1.
	if unsafeIdx(ptr, read) == '-' {
		sign = -1
		read += 1
	}

	value := 0.
	for {
		char := unsafeIdx(ptr, read)
		if char == '.' {
			break
		}
		value = (value * 10) + float64(char-'0')
		read += 1
	}

	read += 1

	m := 1.
	for {
		char := unsafeIdx(ptr, read)
		if char == '\n' {
			break
		}
		m *= 0.1
		value += m * float64(char-'0')
		read += 1
	}

	return read, (value * sign)
}

func parseName(b []byte) (int, name) {
	read := 0
	ptr := unsafe.Pointer(&b[0])

	nameHash := hash(0)
	for unsafeIdx(ptr, read) != ';' {
		nameHash.hashByte(unsafeIdx(ptr, read))
		read += 1
	}

	name := name{
		value: unsafe.Slice(&b[0], read),
		hash:  nameHash,
	}

	return read, name
}

type result struct {
	name  name
	min   float64
	max   float64
	total float64
	count int
}

func newResult(name name) result {
	return result{
		name: name,
		min:  math.MaxFloat64,
		max:  -math.MaxFloat64,
	}
}

func (r result) update(value float64) result {
	r.min = min(r.min, value)
	r.max = max(r.max, value)
	r.total += value
	r.count += 1
	return r
}

func (r result) aggregate(r2 result) result {
	return result{
		name:  r.name,
		min:   min(r.min, r2.min),
		max:   max(r.max, r2.max),
		total: r.total + r2.total,
		count: r.count + r2.count,
	}
}

type name struct {
	value []byte
	hash  hash
}

func (n name) equal(n2 name) bool {
	return bytes.Compare(n.value, n2.value) == 0
}

type hashMap struct {
	mask     int
	occupied []bool
	data     []result
}

func newHashMap(size int) hashMap {
	return hashMap{
		occupied: make([]bool, 1<<size),
		data:     make([]result, 1<<size),
		mask:     (1 << size) - 1,
	}
}

func (hm hashMap) get(name name) (result, bool) {
	idx := int(name.hash) & hm.mask
	for hm.occupied[idx] {
		if hm.data[idx].name.equal(name) {
			return hm.data[idx], true
		}
		idx = (idx + 1) & hm.mask
	}
	return result{}, false
}

func (hm hashMap) set(r result) {
	idx := int(r.name.hash) & hm.mask
	for hm.occupied[idx] {
		if hm.data[idx].name.equal(r.name) {
			break
		}
		idx = (idx + 1) & hm.mask
	}
	hm.occupied[idx] = true
	hm.data[idx] = r
}

type hash int

const fnvPrime = 1099511628211

func (h *hash) hashByte(b byte) {
	*h = (*h ^ hash(b)) * fnvPrime
}

func unsafeIdx(ptr unsafe.Pointer, off int) byte {
	return *(*byte)(unsafe.Add(ptr, off))
}
