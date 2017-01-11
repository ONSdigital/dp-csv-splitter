package main

import (
	"github.com/ONSdigital/dp-csv-splitter/aws"
	"github.com/ONSdigital/dp-csv-splitter/config"
	"github.com/ONSdigital/dp-csv-splitter/message"
	"github.com/ONSdigital/dp-csv-splitter/splitter"
	"github.com/ONSdigital/go-ns/log"
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/gorilla/pat"
	"net/http"
	"os"
	"os/signal"
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
	awsService := aws.NewService()
	csvProcessor := splitter.NewCSVProcessor()

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

	// For now this is redundant but in future we will be adding a health check endpoint.
	router := pat.New()

	go func() {
		if err := http.ListenAndServe(config.BindAddr, router); err != nil {
			log.Error(err, nil)
			os.Exit(1)
		}
	}()

	consumerConfig := cluster.NewConfig()
	consumer, err := cluster.NewConsumer([]string{config.KafkaAddr}, config.KafkaConsumerGroup, []string{config.KafkaConsumerTopic}, consumerConfig)
	message.ConsumerLoop(consumer, awsService, csvProcessor)
}
