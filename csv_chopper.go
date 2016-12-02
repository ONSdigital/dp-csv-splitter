package main

import (
	"github.com/ONSdigital/dp-csv-splitter/model"
	"log"
	"os"
)

const usage = "Usage: ./csv_chopper <csv_file>"

func main() {

	if len(os.Args) != 2 {
		log.Fatal(usage)
	}

	csv_consumer := model.CreateCsvConsumer()

	defer csv_consumer.Close()

	model.Loop(csv_consumer.Reader)

}
