package main

import (
	"log"
	"os"
	"github.com/ONSdigital/dp-csv-splitter/model"
)

const usage = "Usage: ./csv_chopper <csv_file>"

func main() {

	if len(os.Args) != 2 {
		log.Fatal(usage)
	}

	csv_consumer := model.CreateCsvConsumer()
	producer := model.Producer()

	defer csv_consumer.File.Close()
	defer func() {
		if err := producer.Close(); err != nil {
			panic(err)
		}
	}()

	model.Loop(csv_consumer.Reader, producer)
}
