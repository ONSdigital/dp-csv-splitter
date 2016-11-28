package main

import (
	"fmt"
	"log"
	"os"
	"encoding/csv"
	"github.com/ONSdigital/dp-csv-splitter/model"
	"github.com/Shopify/sarama"
)

const usage = "Usage: ./csv_chopper <csv_file>"

func main() {

	if len(os.Args) != 2 {
		log.Fatal(usage)
	}

	csv_location := os.Args[1]
	f, err := os.Open(csv_location)
	if err != nil {
		fmt.Printf("Could not open the csv file %s, %s", csv_location, err.Error())
		os.Exit(1)
	}

	defer f.Close()

	csvr := csv.NewReader(f)

	config := sarama.NewConfig()
	// Return specifies what channels will be populated.
	// If they are set to true, you must read from
	// config.Producer.Return.Successes = true
	// The total number of times to retry sending a message (default 3).
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll
	brokers := []string{"localhost:9092"}
	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			panic(err)
		}
	}()

	model.Loop(csvr, producer)

}
