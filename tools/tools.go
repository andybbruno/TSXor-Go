package tools

import (
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"strconv"
)

//LoadFromCSV loads a csv file in memory and returns a slice of timestamps and a matrix of values
func LoadFromCSV(path string, skipFirstRow bool) ([]int64, [][]float64) {

	csvFile, err := os.Open(path)

	if err != nil {
		fmt.Println(err)
	}

	defer csvFile.Close()

	csvLines, err := csv.NewReader(csvFile).ReadAll()

	if err != nil {
		fmt.Println(err)
	}
	if skipFirstRow {
		csvLines = csvLines[1:]
	}

	timestamps := []float64{}
	values := [][]float64{}

	for _, strLine := range csvLines {
		tmp := []float64{}
		for _, val := range strLine {
			if val != "" {
				s, _ := strconv.ParseFloat(val, 64)
				tmp = append(tmp, s)
			} else {
				s := math.NaN()
				tmp = append(tmp, s)
			}
		}
		timestamps = append(timestamps, tmp[0])
		values = append(values, tmp[1:])
	}

	times := []int64{}

	for _, v := range timestamps {
		times = append(times, int64(v))
	}

	return times, values

	// f, err := os.Create("data.bin")
	// defer f.Close()

	// nLines := len(lines)
	// nCols := len(lines[0])

	// bs := make([]byte, 8)

	// binary.LittleEndian.PutUint64(bs, uint64(nLines))
	// f.Write(bs)
	// binary.LittleEndian.PutUint64(bs, uint64(nCols))
	// f.Write(bs)

}
