package splitter

import (
	"encoding/csv"
	"github.com/ONSdigital/dp-csv-splitter/config"
	"github.com/ONSdigital/dp-csv-splitter/model"
	"os"
	"os/signal"
)

// CSVProcessor defines the CSVProcessor interface.
type CSVProcessor interface {
	Process(csvReader *csv.Reader)
}

// Processor implementation of the CSVProcessor interface.
type Processor struct{}

// NewCSVProcessor create a new Processor.
func NewCSVProcessor() *Processor {
	return &Processor{}
}

// Process implementation of the Process function.
func (p *Processor) Process(csvReader *csv.Reader) {
	producer := model.Producer(config.KafkaAddr)

	defer func() {
		if err := producer.Close(); err != nil {
			panic(err)
		}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	doneCh := make(chan struct{})

	model.Loop(csvReader, producer)

	<-doneCh
}
