#!/bin/bash -eux

export BINPATH=$(pwd)/bin
export GOPATH=$(pwd)/go

pushd $GOPATH/src/github.com/ONSdigital/dp-csv-splitter
  go build -o $BINPATH/dp-csv-splitter
popd
