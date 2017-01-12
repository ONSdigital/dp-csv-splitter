dp-csv-splitter
================

Application retrieves a specified CSV file from AWS S3 bucket, splits it into rows sending each as a individual message
to the configured Kafka Topic to be consumed by the [database-loader]
(https://github.com/ONSdigital/dp-dd-database-loader).

### Getting started

First grab the code

`go get github.com/ONSdigital/dp-csv-splitter`

You will need to have Kafka set up locally. Set the following env variables (the example here uses the default ports)

```
ZOOKEEPER=localhost:2181
KAFKA=localhost:9092
```

Install Kafka:

```
brew install kafka
brew services start kafka
brew services start zookeeper
```

Run the Kafka console consumer
```
kafka-console-consumer --zookeeper $ZOOKEEPER --topic test
```

Run the Kafka console consumer
```
kafka-console-producer --broker-list $KAFKA --topic test
```

Run the the splitter
```
make debug
```

You will need to have access to the ONS DP AWS account and to have AWSCLI installed locally - follow this
[guide](http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-set-up.html)

You will also need to have [dp-dd-file-uploader](https://github.com/ONSdigital/dp-dd-file-uploader]) running to supply
messages for it to consume.

If everything is working correctly the splitter will retrieve the file from the AWS S3 bucket - specified by the
```S3URL``` parameter - split it into individual rows posting each as a kafka message to the outbound kafka topic ready
to be consumed by the [database-loader].

### Configuration

| Environment variable | Default                 | Description
| -------------------- | ----------------------- | ----------------------------------------------------
| BIND_ADDR            | ":21000"                | The host and port to bind to.
| KAFKA_ADDR           | "http://localhost:9092" | The Kafka address to send messages to.
| KAFKA_CONSUMER_GROUP | "file-uploaded"         | The Kafka consumer group to consume messages from.
| KAFKA_CONSUMER_TOPIC | "file-uploaded"         | The Kafka topic to consume messages from.
| AWS_REGION           | "eu-west-1"             | The AWS region to use.
| TOPIC_NAME           | "test"                  | The name of the Kafka topic to send the messages to.

### Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for details.

### License

Copyright ©‎ 2016, Office for National Statistics (https://www.ons.gov.uk)

Released under MIT license, see [LICENSE](LICENSE.md) for details.
