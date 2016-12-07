package splitter

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/ONSdigital/dp-csv-splitter/config"
	"github.com/ONSdigital/go-ns/log"
	"github.com/Shopify/sarama"
	"io"
	"strconv"
	"strings"
	"time"
)

// NewKafkaProducer is a factory method for instances of AsyncProducer
var NewKafkaProducer func() sarama.AsyncProducer = newKafkaProducer

func newKafkaProducer() sarama.AsyncProducer {
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Producer.Retry.Max = 5
	kafkaConfig.Producer.RequiredAcks = sarama.WaitForAll
	//kafkaConfig.Producer.Return.Successes = true

	producer, err := sarama.NewAsyncProducer([]string{config.KafkaAddr}, kafkaConfig)
	if err != nil {
		log.Error(err, log.Data{"message": "Failed to create message producer."})
	}

	go func() {
		for err := range producer.Errors() {
			log.Debug("Error sending CSV line to Kafka", log.Data{
				"error": err.Err.Error(),
			})
		}

		log.Debug("Error range ended", log.Data{})
	}()

	go func() {
		for success := range producer.Successes() {
			log.Debug("Success sending csv line to Kafka", log.Data{
				"offset": success.Offset,
			})
		}
		log.Debug("Success range ended", log.Data{})
	}()

	return producer
}

// CSVProcessor defines the CSVProcessor interface.
type CSVProcessor interface {
	Process(r io.Reader)
}

// Processor implementation of the CSVProcessor interface.
type Processor struct{}

// NewCSVProcessor create a new Processor.
func NewCSVProcessor() *Processor {
	return &Processor{}
}

type Message struct {
	Index int    `json:"index"`
	Row   string `json:"datapoint"`
}

func createMessage(index int, row []string) Message {
	return Message{Index: index, Row: strings.Join(row[:], ",")}
}

func (p *Processor) Process(r io.Reader) {

	kafkaProducer := NewKafkaProducer()
	defer func() {
		if err := kafkaProducer.Close(); err != nil {
			log.Error(err, log.Data{})
		}
	}()

	csvR := csv.NewReader(r)
	var index = 0

csvLoop:
	for {
		row, err := csvR.Read()
		if err != nil {
			if err == io.EOF {
				log.Debug("EOF reached, no more records to process", nil)
				break csvLoop
			} else {
				fmt.Println("Error occored and cannot process anymore entry", err.Error())
				panic(err)
			}
		}

		messageJSON, err := json.Marshal(createMessage(index, row))

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

		index++
		kafkaProducer.Input() <- producerMsg
	}

	log.Debug("Kafka Loop details", log.Data{
		"Enqueued": index,
	})
}
