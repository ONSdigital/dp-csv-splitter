package handlers

import (
	"encoding/json"
	"github.com/ONSdigital/dp-csv-splitter/aws"
	"github.com/ONSdigital/dp-csv-splitter/splitter"
	"io"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"errors"
	"github.com/ONSdigital/go-ns/log"
)

const csvFileExt = ".csv"

type requestBodyReader func(r io.Reader) ([]byte, error)

// SplitterResponse struct defines the response for the /splitter API.
type SplitterResponse struct {
	Message string `json:"message,omitempty"`
}

// SplitterRequest struct defines a splitter request
type SplitterRequest struct {
	FilePath string `json:"filePath"`
}

var unsupoprtedFileTypeErr = errors.New("Unspported file type.")
var awsClientErr = errors.New("Error while attempting get to get from from AWS s3 bucket.")
var filePathParamMissingErr = errors.New("No filePath value was provided.")
var awsService = aws.NewService()
var csvProcessor splitter.CSVProcessor = splitter.NewCSVProcessor()
var readSplitterRequestBody requestBodyReader = ioutil.ReadAll

// Responses
var splitterRespReadReqBodyErr = SplitterResponse{"Error when attempting to read request body."}
var splitterRespUnmarshalBody = SplitterResponse{"Error when attempting to unmarshal request body."}
var splitterRespFilePathMissing = SplitterResponse{"No filePath parameter was specified in the request body."}
var splitterRespUnsupportedFileType = SplitterResponse{"Unspported file type. Please specify a filePath for a .csv file."}
var splitterResponseSuccess = SplitterResponse{"Your request is being processed."}

// Handle CSV splitter handler. Get the requested file from AWS S3, split it and send each row to the configured Kafka Topic.
func Handle(w http.ResponseWriter, req *http.Request) {
	bytes, err := readSplitterRequestBody(req.Body)
	defer req.Body.Close()

	if err != nil {
		log.Error(err, nil)
		WriteResponse(w, splitterRespReadReqBodyErr, http.StatusBadRequest)
		return
	}

	var splitterReq SplitterRequest
	if err := json.Unmarshal(bytes, &splitterReq); err != nil {
		log.Error(err, nil)
		WriteResponse(w, splitterRespUnmarshalBody, http.StatusBadRequest)
		return
	}

	if len(splitterReq.FilePath) == 0 {
		log.Error(filePathParamMissingErr, nil)
		WriteResponse(w, splitterRespFilePathMissing, http.StatusBadRequest)
		return
	}

	if fileType := filepath.Ext(splitterReq.FilePath); fileType != csvFileExt {
		log.Error(unsupoprtedFileTypeErr, log.Data{"expected": csvFileExt, "actual": fileType})
		WriteResponse(w, splitterRespUnsupportedFileType, http.StatusBadRequest)
		return
	}
	awsReader, err := awsService.GetCSV(splitterReq.FilePath)
	if err != nil {
		log.Error(awsClientErr, log.Data{"details": err.Error()})
		WriteResponse(w, SplitterResponse{err.Error()}, http.StatusBadRequest)
		return
	}
	csvProcessor.Process(awsReader)
	WriteResponse(w, splitterResponseSuccess, http.StatusOK)
}

func setReader(reader requestBodyReader) {
	readSplitterRequestBody = reader
}

func setCSVProcessor(p splitter.CSVProcessor) {
	csvProcessor = p
}

func setAWSClient(c aws.AWSService) {
	awsService = c
}
