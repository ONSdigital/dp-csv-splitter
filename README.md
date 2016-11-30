dp-csv-splitter
================

A utility program that aims to stream a large CSV file and splits it into Kafka messages to be consumed by the
[database-loader](https://github.com/ONSdigital/dp-dd-database-loader).

### Getting started

First grab the code

`go get github.com/ONSdigital/dp-csv-splitter`

Once in the directory, compile and run the program

```
make debug
```

The project includes a small data set in the `sample_csv` directory for test usage.

### Configuration

| Environment variable | Default                 | Description
| -------------------- | ----------------------- | ----------------------------------------------------
| BIND_ADDR            | ":21000"                | The host and port to bind to.
| KAFKA_ADDR           | "http://localhost:9092" | The Kafka address to send messages to.
| S3_BUCKET            | "dp-csv-splitter-1"     | The name of AWS S3 bucket to get the csv files from.
| AWS_REGION           | "eu-west-1"             | The AWS region to use.
| TOPIC_NAME           | "test"                  | The name of the Kafka topic to send the messages to.

### Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for details.

### License

Copyright ©‎ 2016, Office for National Statistics (https://www.ons.gov.uk)

Released under MIT license, see [LICENSE](LICENSE.md) for details.
