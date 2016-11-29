package main

import (
	"log"
	"os"
	"github.com/ONSdigital/dp-csv-splitter/model"
	"os/signal"
)

const usage = "Usage: ./csv_chopper <csv_file>"

func main() {

	if len(os.Args) != 2 {
		log.Fatal(usage)
	}

	csv_consumer := model.CreateCsvConsumer()
	producer := model.Producer()

	defer csv_consumer.Close()
	defer func() {
		if err := producer.Close(); err != nil {
			panic(err)
		}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	doneCh := make(chan struct{})

	model.Loop(csv_consumer.Reader, producer)

	<-doneCh
}
