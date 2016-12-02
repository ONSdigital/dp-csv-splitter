package main

import (
	"github.com/ONSdigital/dp-csv-splitter/model"
	"log"
	"os"
	"os/signal"
)

const usage = "Usage: ./csv_chopper <csv_file>"

func main() {

	if len(os.Args) != 2 {
		log.Fatal(usage)
	}

	csv_consumer := model.CreateCsvConsumer()

	defer csv_consumer.Close()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	doneCh := make(chan struct{})

	model.Loop(csv_consumer.Reader)

	<-doneCh
}
