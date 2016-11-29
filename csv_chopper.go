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

	addr := "localhost:9092"

	if v := os.Getenv("KAFKA_ADDR"); len(v) > 0 {
		addr = v
	}

	csv_consumer := model.CreateCsvConsumer()
	producer := model.Producer(addr)

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
