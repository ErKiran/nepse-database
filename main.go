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
	// Code to measure
	index := 0
	err := filepath.Walk("nepse-data/data/company-wise", func(path string, info fs.FileInfo, err error) error {
		// channel := make(chan string, 100)
		// for rec := range Work(path) {
		// 	fmt.Println(rec, "rec")
		// }
		// Work(path)
		for rec := range Work(path) {
			fmt.Println("rec", rec)
		}
		index++
		return nil
	})

	fmt.Println("total", index)
	if err != nil {
		fmt.Println("err", err)
	}
	duration := time.Since(start)
	fmt.Println("duration", duration)

}

func Work(path string) (ch chan Candle) {
	isCsv, ticker := IsCsv(path)
	fmt.Println("ticker", ticker)
	ch = make(chan Candle, 10)
	// var priceHistory []Candle
	go func() {
		if isCsv {
			f, err := os.Open(path)
			if err != nil {
				log.Fatal("Unable to read input file "+path, err)
			}

			defer close(ch)
			csvReader := csv.NewReader(f)

			defer f.Close()

			// record, err := csvReader.Read()
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
				fmt.Println("candle", candle)
				ch <- candle
			}
		}
	}()
	return
}

func WorkN(path string) {
	isCsv, ticker := IsCsv(path)
	fmt.Println("ticker", ticker)
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
