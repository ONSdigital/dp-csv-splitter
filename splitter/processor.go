package splitter

import (
	"bufio"
	"encoding/json"
	"github.com/ONSdigital/dp-csv-splitter/config"
	"github.com/ONSdigital/go-ns/log"
	"github.com/Shopify/sarama"
	"io"
	"strconv"
	"time"
)

var Producer sarama.AsyncProducer

// CSVProcessor defines the CSVProcessor interface.
type CSVProcessor interface {
	Process(r io.Reader, filename string, startTime time.Time, datasetID string)
}

// Processor implementation of the CSVProcessor interface.
type Processor struct{}

// NewCSVProcessor create a new Processor.
func NewCSVProcessor() *Processor {
	return &Processor{}
}

type Message struct {
	Index     int    `json:"index"`
	Row       string `json:"row"`
	Filename  string `json:"filename"`
	StartTime int64  `json:"startTime"`
	DatasetID string `json:"datasetID"`
}

func (p *Processor) Process(r io.Reader, filename string, startTime time.Time, datasetID string) {

	scanner := bufio.NewScanner(r)
	var rowIndex = 0
	var errorCount = 0

csvLoop:
	for {
		scanSuccessful := scanner.Scan()
		if !scanSuccessful {
			log.Debug("EOF reached, no more records to process", nil)
			break csvLoop
		}

		message := Message{
			Index:     rowIndex,
			Row:       scanner.Text(),
			Filename:  filename,
			StartTime: startTime.UTC().Unix(),
			DatasetID: datasetID,
		}

		messageJSON, err := json.Marshal(message)
		if err != nil {
			log.Error(err, log.Data{
				"details": "Could not create the json representation of message",
				"message": messageJSON,
			})
			panic(err)
		}

		strTime := strconv.Itoa(int(time.Now().Unix()))
		producerMsg := &sarama.ProducerMessage{
			Topic: config.TopicName,
			Key:   sarama.StringEncoder(strTime),
			Value: sarama.ByteEncoder(messageJSON),
		}

		select {
		case Producer.Input() <- producerMsg:
			rowIndex++
		case err := <-Producer.Errors():
			errorCount++
			log.Error(err, nil)
		}
	}

	log.Debug("Kafka Loop details", log.Data{
		"Enqueued": rowIndex,
		"Errors":   errorCount,
	})
}
