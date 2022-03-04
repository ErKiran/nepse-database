package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Candle struct {
	Date   string
	Ticker string
	Open   string
	Close  string
	High   string
	Low    string
	Volume string
}

func main() {
	start := time.Now()
	fmt.Println("start", start)
	channel := make(chan Candle, 1000)
	err := filepath.Walk("nepse-data/data/company-wise", func(path string, info fs.FileInfo, err error) error {
		go SimpleWorker(path, channel)
		// WorkN(path)
		return nil
	})

	for elem := range channel {
		fmt.Println(elem)
	}

	if err != nil {
		fmt.Println("err", err)
	}
	duration := time.Since(start)
	fmt.Println("duration", duration)
}

func SimpleWorker(path string, channel chan Candle) {
	isCsv, ticker := IsCsv(path)
	go func() {
		if isCsv {
			f, err := os.Open(path)
			if err != nil {
				log.Fatal("Unable to read input file "+path, err)
			}

			defer f.Close()

			csvReader := csv.NewReader(f)

			if _, err := csvReader.Read(); err != nil { //read header
				log.Fatal(err)
			}
			for {
				rec, err := csvReader.Read()
				if err != nil {
					if err == io.EOF {
						break
					}
					log.Fatal(err)
				}
				candle := Candle{
					Ticker: ticker,
					Low:    rec[3],
					High:   rec[2],
					Open:   rec[1],
					Close:  rec[4],
					Volume: rec[6],
					Date:   rec[0],
				}
				channel <- candle
			}
			defer close(channel)
		}
	}()

	return
}

func WorkN(path string) {
	isCsv, ticker := IsCsv(path)
	// var priceHistory []Candle
	if isCsv {
		f, err := os.Open(path)
		if err != nil {
			log.Fatal("Unable to read input file "+path, err)
		}
		defer f.Close()

		csvReader := csv.NewReader(f)

		// record, err := csvReader.Read()

		for {
			rec, err := csvReader.Read()
			if err != nil {
				if err == io.EOF {
					break
				}
				log.Fatal(err)
			}
			candle := Candle{
				Ticker: ticker,
				Low:    rec[3],
				High:   rec[2],
				Open:   rec[1],
				Close:  rec[4],
				Volume: rec[6],
				Date:   rec[0],
			}
			fmt.Println("candle", candle)
		}

		// fmt.Println(records)
	}

}

func IsCsv(path string) (bool, string) {
	splittedPath := strings.Split(path, "/")

	lastItem := splittedPath[len(splittedPath)-1]

	if strings.Contains(lastItem, ".csv") {
		return true, strings.Split(lastItem, ".")[0]
	}
	return false, ""
}
