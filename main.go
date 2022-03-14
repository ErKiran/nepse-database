package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"io/fs"
	"log"
	"nepse-database/models"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/lib/pq"
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

func setupDB() *sql.DB {
	var err error
	db, err := sql.Open("postgres", "postgres://postgres:password@127.0.0.1:5432/kiran?sslmode=disable")
	if err != nil {
		log.Fatal(err, "Unable to connect database")
	}
	return db
}

func main() {
	start := time.Now()
	fmt.Println("start", start)
	var candleData []interface{}
	var placeholders []string
	err := filepath.Walk("nepse-data/data/company-wise", func(path string, info fs.FileInfo, err error) error {
		candles := WorkN(path)
		for index, candle := range candles {
			placeholders = append(placeholders, fmt.Sprintf("($%d,$%d,$%d, $%d, $%d, $%d, $%d)",
				index*3+1,
				index*3+2,
				index*3+3,
				index*3+4,
				index*3+5,
				index*3+6,
				index*3+7,
			))
			candleData = append(candleData, candle.Date, candle.Ticker, candle.High, candle.Low, candle.Open, candle.Close, candle.Volume)
		}
		return nil
	})

	if err != nil {
		fmt.Println("errrrr", err)
	}

	var nepse models.NepseData

	db := setupDB()

	err = nepse.BulkInsert(db, placeholders, candleData)

	if err != nil {
		fmt.Println("errrrr", err)
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

func WorkN(path string) []Candle {
	isCsv, ticker := IsCsv(path)
	var priceHistory []Candle
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
			priceHistory = append(priceHistory, candle)
		}
		return priceHistory
	}
	return priceHistory
}

func IsCsv(path string) (bool, string) {
	splittedPath := strings.Split(path, "/")

	lastItem := splittedPath[len(splittedPath)-1]

	if strings.Contains(lastItem, ".csv") {
		return true, strings.Split(lastItem, ".")[0]
	}
	return false, ""
}
