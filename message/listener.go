package message

import (
	"encoding/json"
	"github.com/ONSdigital/dp-csv-splitter/aws"
	"github.com/ONSdigital/dp-csv-splitter/message/event"
	"github.com/ONSdigital/dp-csv-splitter/splitter"
	"github.com/ONSdigital/go-ns/log"
	"github.com/Shopify/sarama"
	"github.com/satori/go.uuid"
	"time"
)

func ConsumerLoop(listener Listener, awsService aws.AWSService, processor splitter.CSVProcessor) {
	for message := range listener.Messages() {
		log.Debug("Message received from Kafka!", nil)
		processMessage(message, awsService, processor)
	}
}

func processMessage(message *sarama.ConsumerMessage, awsService aws.AWSService, csvProcessor splitter.CSVProcessor) error {

	var uploadEvent event.UploadEvent
	if err := json.Unmarshal(message.Value, &uploadEvent); err != nil {
		log.Error(err, nil)
		return err
	}
	awsReader, err := awsService.GetCSV(uploadEvent)
	if err != nil {
		log.Error(err, log.Data{"message": "Error while attempting get to get from from AWS s3 bucket."})
		return err
	}

	datasetId := uuid.NewV4().String()
	csvProcessor.Process(awsReader, uploadEvent, time.Now(), datasetId)
	return nil
}

type Listener interface {
	Messages() <-chan *sarama.ConsumerMessage
}
