package main

import (
	"github.com/ONSdigital/dp-csv-splitter/config"
	"github.com/ONSdigital/dp-csv-splitter/handlers"
	"github.com/ONSdigital/go-ns/log"
	"github.com/gorilla/pat"
	"net/http"
	"os"
)

func main() {
	router := pat.New()
	router.Post(handlers.SplitterURI, handlers.Handle)

	if err := http.ListenAndServe(config.BindAddr, router); err != nil {
		log.Error(err, nil)
		os.Exit(1)
	}
}
