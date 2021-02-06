package main

import (
	"fmt"
	"os"
	"time"
	"tools"
	"tsxor"
)

func main() {
	timestamps, values := tools.LoadFromCSV(os.Args[1], true)

	start := time.Now()
	c := tsxor.Compressor{}
	c.New(timestamps[0])
	for i := 0; i < len(values); i++ {
		c.AddValue(timestamps[i], &values[i])
	}
	c.Close()
	t := time.Now()
	elapsed := t.Sub(start)
	nCols := len(values[0])
	nLines := len(values)
	originalSize := 64 * (nCols + 1) * nLines
	compressedSize := c.CompressedSize()

	fmt.Println("\n\n\n*** COMPRESSION ***")
	th := (float64(8*(nCols+1)*nLines) / float64(elapsed.Microseconds()))
	fmt.Printf("%.3f %s\n", th, " MB/s")

	ratio := float64(originalSize) / float64(compressedSize)
	fmt.Printf("%.3f%s\n", ratio, " x")

	start = time.Now()
	bstream, bytes := c.ExportData()
	d := tsxor.Decompressor{}
	d.New(bstream, bytes, uint64(nCols))
	for d.HasNext() {

	}
	t = time.Now()
	elapsed = t.Sub(start)

	fmt.Println("\n\n\n*** DECOMPRESSION ***")
	th = 8 * (float64((nCols + 1) * nLines)) / float64((elapsed.Microseconds()))
	fmt.Printf("%.3f %s\n\n", th, " MB/s")

	fmt.Println("*** LAST ROW ***")
	fmt.Print(d.StoredTimestamp, "-->")
	for _, v := range d.StoredValues {
		fmt.Print(v, "|")
	}
	fmt.Println("")
}
