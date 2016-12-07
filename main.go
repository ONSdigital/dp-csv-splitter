package main

import (
	"github.com/ONSdigital/dp-csv-splitter/config"
	"github.com/ONSdigital/dp-csv-splitter/handlers"
	"github.com/ONSdigital/go-ns/log"
	"github.com/gorilla/pat"
	"net/http"
	"os"
	"github.com/ONSdigital/dp-csv-splitter/splitter"
	"github.com/Shopify/sarama"
	"os/signal"
)

func main() {
	config.Load()

	// Trap SIGINT to trigger a graceful shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Producer.Retry.Max = 5
	kafkaConfig.Producer.RequiredAcks = sarama.WaitForLocal
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

	if err := http.ListenAndServe(config.BindAddr, router); err != nil {
		log.Error(err, nil)
		os.Exit(1)
	}
}
