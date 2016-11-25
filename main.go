package main

import (
	"os"
	"github.com/gorilla/pat"
	"net/http"
	"github.com/ONSdigital/dp-csv-splitter/handlers"
	"github.com/ONSdigital/go-ns/log"
)

func main() {

	bindAddr := os.Getenv("BIND_ADDR")
	if len(bindAddr) == 0 {
		bindAddr = ":21000"
	}

	fileProviderUrl := os.Getenv("FILE_PROVIDER_URL")
	if len(fileProviderUrl) == 0 {
		fileProviderUrl = ""
	}

	router := pat.New()
	router.Post("/chopper", handlers.Handle)

	log.Debug("Configuration", log.Data{
		"BIND_ADDR": bindAddr,
		"FILE_PROVIDER_URL": fileProviderUrl,
	})

	if err := http.ListenAndServe(bindAddr, router); err != nil {
		log.Error(err, nil)
		os.Exit(1)
	}
}