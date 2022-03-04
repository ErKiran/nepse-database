package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math"
	"strconv"
	"strings"
)

func calculateScore(f float64) float64 {
	return math.Sqrt(f)
}

func processCSV(rc io.Reader) (ch chan []string) {
	ch = make(chan []string, 10)
	go func() {
		r := csv.NewReader(rc)
		if _, err := r.Read(); err != nil { //read header
			log.Fatal(err)
		}
		defer close(ch)
		for {
			rec, err := r.Read()
			if err != nil {
				if err == io.EOF {
					break
				}
				log.Fatal(err)

			}
			ch <- rec
		}
	}()
	return
}

func main() {
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)

	if err := w.Write([]string{"timestamp", "value", "score"}); err != nil {
		log.Fatal(err)
	}

	for rec := range processCSV(strings.NewReader(data)) {
		f, err := strconv.ParseFloat(rec[1], 64)
		if err != nil {
			log.Fatal("Record: %v, Error: %v", rec, err)
		}

		// calculate score
		score := calculateScore(f)

		scoreStr := strconv.FormatFloat(score, 'f', 8, 64)
		rec = append(rec, scoreStr)
		if err = w.Write(rec); err != nil {
			log.Fatal(err)
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		log.Fatal(err)
	}
	fmt.Println(buf.String())
}

const data = `timestamp,value
5/27/14 12:00,9.96370968
5/27/14 12:05,9.38186666
6/19/14 19:25,1.71673333
6/19/14 19:30,2.6974
`
