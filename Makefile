build:
	go build -o build/dp-csv-splitter

debug: build
	HUMAN_LOG=1 ./build/dp-csv-splitter

.PHONY: build debug
