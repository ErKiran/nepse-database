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

	"github.com/joho/godotenv"

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
	db, err := sql.Open(os.Getenv("DIALECT"), os.Getenv("DB_URI"))
	if err != nil {
		log.Fatal(err, "Unable to connect database")
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS nepse(
		date character varying(70) NOT NULL DEFAULT '',
		ticker character varying(70) NOT NULL DEFAULT '',
		high character varying(70) NOT NULL DEFAULT '',
		close character varying(70) NOT NULL DEFAULT '',
		low character varying(70) NOT NULL DEFAULT '',
		volume character varying(70) NOT NULL DEFAULT '',
		open character varying(70) NOT NULL DEFAULT '');`)

	if err != nil {
		log.Fatal(err, "Unable to create nepse data")
	}
	return db
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	start := time.Now()
	defer func() { fmt.Println("duration", time.Since(start)) }()
	var candleData models.Candles
	err = filepath.Walk("nepse-data/data/company-wise", func(path string, info fs.FileInfo, err error) error {
		candles := WorkN(path)
		for _, candle := range candles {
			candleData.Volume = append(candleData.Volume, candle.Volume)
			candleData.Open = append(candleData.Open, candle.Open)
			candleData.Close = append(candleData.Close, candle.Close)
			candleData.High = append(candleData.High, candle.High)
			candleData.Low = append(candleData.Low, candle.Low)
			candleData.Date = append(candleData.Date, candle.Date)
			candleData.Ticker = append(candleData.Ticker, candle.Ticker)
		}
		return nil
	})

	if err != nil {
		fmt.Println("errrrr", err)
	}

	var nepse models.NepseData

	db := setupDB()

	err = nepse.BulkInsert(db, candleData)

	if err != nil {
		fmt.Println("errrrr", err)
	}
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

		// This will skip the header of each file
		_, err = csvReader.Read()

		if err != nil {
			log.Fatal("Unable to read input file "+path, err)
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
