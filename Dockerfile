FROM ubuntu:16.04

WORKDIR /app/

COPY ./build/dp-csv-splitter .

ENTRYPOINT ./dp-csv-splitter
