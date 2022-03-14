package models

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
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

type NepseData []Nepse

func (nd NepseData) BulkInsert(db *sql.DB, placeholders []string, vals []interface{}) error {
	txn, err := db.Begin()
	if err != nil {
		return errors.New("could not start a new transaction")
	}

	insertStatement := fmt.Sprintf("INSERT INTO nepse(date,ticker,high,low,open,close,volume) VALUES %s", strings.Join(placeholders, ","))
	_, err = txn.Exec(insertStatement, vals...)
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
