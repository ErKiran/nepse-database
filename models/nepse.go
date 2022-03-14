package models

import (
	"database/sql"
	"errors"
	"fmt"

	pq "github.com/lib/pq"
)

type Nepse struct {
	Date   string `db:"date"`
	Ticker string `db:"ticker"`
	Open   string `db:"open"`
	Close  string `db:"close"`
	High   string `db:"high"`
	Low    string `db:"low"`
	Volume string `db:"volume"`
}

type Candles struct {
	Date   []string
	Ticker []string
	Open   []string
	Close  []string
	High   []string
	Low    []string
	Volume []string
}

type NepseData []Nepse

func (nd NepseData) BulkInsert(db *sql.DB, vals Candles) error {
	txn, err := db.Begin()
	if err != nil {
		return errors.New("could not start a new transaction")
	}

	query := `
	INSERT INTO nepse (date, ticker, open, close, high, low, volume)
	  (
		select * from unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::text[])
	  )`
	_, err = txn.Exec(query, pq.Array(vals.Date), pq.Array(vals.Ticker), pq.Array(vals.Open), pq.Array(vals.Close), pq.Array(vals.High), pq.Array(vals.Low), pq.Array(vals.Volume))
	if err != nil {
		txn.Rollback()
		fmt.Println("err", err)
		return errors.New("failed to insert multiple records at once")
	}

	if err := txn.Commit(); err != nil {
		return errors.New("failed to commit transaction")
	}

	return nil
}
