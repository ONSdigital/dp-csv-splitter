package main

import (
	"github.com/ONSdigital/dp-csv-splitter/config"
	"github.com/ONSdigital/dp-csv-splitter/handlers"
	"github.com/ONSdigital/dp-csv-splitter/splitter"
	"github.com/ONSdigital/go-ns/log"
	"github.com/Shopify/sarama"
	"github.com/gorilla/pat"
	"net/http"
	"os"
	"os/signal"
	"github.com/bsm/sarama-cluster"
	"encoding/json"
)

func main() {
	config.Load()

	// Trap SIGINT to trigger a graceful shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Producer.Retry.Max = 5
	kafkaConfig.Producer.RequiredAcks = sarama.WaitForAll
	kafkaConfig.Producer.Return.Successes = true
	kafkaConfig.Producer.Return.Errors = true

	producer, err := sarama.NewSyncProducer([]string{config.KafkaAddr}, kafkaConfig)
	if err != nil {
		log.Error(err, log.Data{"message": "Failed to create message producer."})
	}

	splitter.Producer = producer

	go func() {
		<-signals

		if err := producer.Close(); err != nil {
			log.Debug("Failed to shutdown AsyncProducer gracefully.", nil)
			log.Error(err, nil)
			os.Exit(1)
		}
		log.Debug("Graceful shutdown of AsyncProducer was successful.", nil)
		os.Exit(0)
	}()

	router := pat.New()
	router.Post("/splitter", handlers.Handle)

	go func() {
		if err := http.ListenAndServe(config.BindAddr, router); err != nil {
			log.Error(err, nil)
			os.Exit(1)
		}
	}()

	consumerConfig := cluster.NewConfig()
	consumer, err := cluster.NewConsumer([]string{config.KafkaAddr}, config.KafkaConsumerGroup, []string{config.KafkaConsumerTopic}, consumerConfig)

	go func() {
		for err := range consumer.Errors() {
			log.Error(err, nil)
		}
	}()

	for message := range consumer.Messages() {

		log.Debug("Message received from Kafka!", nil)

		var fileUploadedEvent FileUploaded
		if err := json.Unmarshal(message.Value, &fileUploadedEvent); err != nil {
			log.Error(err, nil)
			continue // replace with return
		}

		log.Debug("Message filename:" + fileUploadedEvent.Filename, nil)

		handlers.ProcessCsv(fileUploadedEvent.Filename)
	}
}

// FileUploaded event
type FileUploaded struct {
	Filename string `json:"filename"`
	Time     int64  `json:"time"`
}