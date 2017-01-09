package config

import (
	"github.com/ONSdigital/go-ns/log"
	"os"
	"strconv"
)

const bindAddrKey = "BIND_ADDR"
const kafkaAddrKey = "KAFKA_ADDR"
const kafkaConsumerGroup = "KAFKA_CONSUMER_GROUP"
const kafkaConsumerTopic = "KAFKA_CONSUMER_TOPIC"
const awsRegionKey = "AWS_REGION"
const topicNameKey = "TOPIC_NAME"
const batchSizeKey = "BATCH_SIZE"

// BindAddr the address to bind to.
var BindAddr = ":21000"

// KafkaAddr the Kafka address to send messages to.
var KafkaAddr = "localhost:9092"

// KafkaConsumerGroup the consumer group to consume messages from.
var KafkaConsumerGroup = "file-uploaded"

// KafkaConsumerTopic the name of the topic to consume messages from.
var KafkaConsumerTopic = "file-uploaded"

// S3Bucket the name of the AWS s3 bucket to get the CSV files from.
var S3Bucket = "dp-csv-splitter"

// AWSRegion the AWS region to use.
var AWSRegion = "eu-west-1"

// TopicName the name of the Kafka topic to send messages to.
var TopicName = "test"

// BatchSize the number of CSV lines to process in a single batch.
var BatchSize int = 100

func init() {
	if bindAddrEnv := os.Getenv(bindAddrKey); len(bindAddrEnv) > 0 {
		BindAddr = bindAddrEnv
	}

	if kafkaAddrEnv := os.Getenv(kafkaAddrKey); len(kafkaAddrEnv) > 0 {
		KafkaAddr = kafkaAddrEnv
	}

	if awsRegionEnv := os.Getenv(awsRegionKey); len(awsRegionEnv) > 0 {
		AWSRegion = awsRegionEnv
	}

	if topicNameEnv := os.Getenv(topicNameKey); len(topicNameEnv) > 0 {
		TopicName = topicNameEnv
	}

	if consumerGroupEnv := os.Getenv(kafkaConsumerGroup); len(consumerGroupEnv) > 0 {
		KafkaConsumerGroup = consumerGroupEnv
	}

	if consumerTopicEnv := os.Getenv(kafkaConsumerTopic); len(consumerTopicEnv) > 0 {
		KafkaConsumerTopic = consumerTopicEnv
	}

	batchSizeEnv, err := strconv.Atoi(os.Getenv(batchSizeKey))
	if err != nil {
		log.Error(err, log.Data{"message": "Failed to parse batch size. Using default."})
	} else {
		BatchSize = batchSizeEnv
	}
}

func Load() {
	// Will call init().
	log.Debug("dp-csv-splitter Configuration", log.Data{
		bindAddrKey:        BindAddr,
		kafkaAddrKey:       KafkaAddr,
		kafkaConsumerGroup: KafkaConsumerGroup,
		kafkaConsumerTopic: KafkaConsumerTopic,
		awsRegionKey:       AWSRegion,
		topicNameKey:       TopicName,
		batchSizeKey:       BatchSize,
	})
}
