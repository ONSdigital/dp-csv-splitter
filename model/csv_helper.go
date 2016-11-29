package model

import (
	"os"
	"encoding/csv"
	"fmt"
)

type CsvConsumer struct {
	ioReader *os.File
	Reader   *csv.Reader
}

func CreateCsvConsumer() *CsvConsumer {
	csv_location := os.Args[1]
	f, err := os.Open(csv_location)
	if err != nil {
		fmt.Printf("Could not open the csv file %s, %s", csv_location, err.Error())
		os.Exit(1)
	}

	return &CsvConsumer{f, csv.NewReader(f) }
}

func (c *CsvConsumer) Close() {
	*c.ioReader.Close()
}