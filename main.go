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
	channel := make(chan Candle, 100)
	err := filepath.Walk("nepse-data/data/company-wise", func(path string, info fs.FileInfo, err error) error {

		// for rec := range Work(path) {
		// 	fmt.Println(rec, "rec")
		// }
		// Work(path)
		go SimpleWorker(path, channel)
		index++
		return nil
	})

	for i := 0; i < index-1; i++ {
		fmt.Println(<-channel)
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
			defer close(channel)
			csvReader := csv.NewReader(f)

			defer f.Close()
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
				channel <- candle
			}
		}
	}()
	return
}

// func Work(path string) (ch chan Candle) {
// 	isCsv, ticker := IsCsv(path)
// 	fmt.Println("ticker", ticker)
// 	ch = make(chan Candle, 10)
// 	// var priceHistory []Candle
// 	go func() {
// 		if isCsv {
// 			f, err := os.Open(path)
// 			if err != nil {
// 				log.Fatal("Unable to read input file "+path, err)
// 			}

// 			defer close(ch)
// 			csvReader := csv.NewReader(f)

// 			defer f.Close()

// 			// record, err := csvReader.Read()
// 			if _, err := csvReader.Read(); err != nil { //read header
// 				log.Fatal(err)
// 			}
// 			for {
// 				rec, err := csvReader.Read()
// 				if err != nil {
// 					if err == io.EOF {
// 						break
// 					}
// 					log.Fatal(err)
// 				}
// 				candle := Candle{
// 					Ticker: ticker,
// 					Low:    rec[3],
// 					High:   rec[2],
// 					Open:   rec[1],
// 					Close:  rec[4],
// 					Volume: rec[6],
// 					Date:   rec[0],
// 				}
// 				fmt.Println("candle", candle)
// 				ch <- candle
// 			}
// 		}
// 	}()
// 	return
// }

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
