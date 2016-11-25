dp-csv-splitter
================

A utility program that aims to stream a large CSV file and splits it into Kafka messages to be consumed by the
[database-loader](https://github.com/ONSdigital/dp-dd-database-loader).

### Getting started

First grab the code

`go get github.com/ONSdigital/dp-csv-splitter`

Once in the directory, compile and run the program

```
go build csv_chopper.go
./csv_chopper <path_to_large_csv>
```

The project includes a small data set in the `sample_csv` directory for test usage.

### Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for details.

### License

Copyright ©‎ 2016, Office for National Statistics (https://www.ons.gov.uk)

Released under MIT license, see [LICENSE](LICENSE.md) for details.
