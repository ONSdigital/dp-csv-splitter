package handlers

import (
	"encoding/csv"
	"encoding/json"
	"github.com/ONSdigital/dp-csv-splitter/aws"
	"github.com/ONSdigital/dp-csv-splitter/splitter"
	"github.com/ONSdigital/go-ns/log"
	"io"
	"io/ioutil"
	"net/http"
)

// URIParamMissingMsg error message for URI parameter missing from request body
const URIParamMissingMsg = "Please specify a uri parameter."

// SuccessMsg success message for splitter requests.
const SuccessMsg = "Your file has been sent to the Chopper"

// ReadRequestBodyErrMsg error message for case where the request body could not be read.
const ReadRequestBodyErrMsg = "Could not read body"

// UnmarshalBodyErrMsg error message for any errors attempting to unmarshalling the request body.
const UnmarshalBodyErrMsg = "Failed to unmarshal body"

// SplitterURI the URI of the CSV splitter endpoint.
const SplitterURI = "/splitter"

// ByteSliceReader type for reading byte slices.
type ByteSliceReader func(r io.Reader) ([]byte, error)

var awsClient = aws.NewClient()
var csvProcessor splitter.CSVProcessor = splitter.NewCSVProcessor()
var requestBodyReader ByteSliceReader = ioutil.ReadAll

// Handle CSV splitter handler. Get the requested file from AWS S3, split it and send each row to the configured Kafka Topic.
func Handle(w http.ResponseWriter, req *http.Request) {
	bytes, err := requestBodyReader(req.Body)
	if err != nil {
		WriteResponse(w, SplitterResponse{ReadRequestBodyErrMsg}, 400)
		return
	}

	var chopperReq SplitterRequest
	if err := json.Unmarshal(bytes, &chopperReq); err != nil {
		WriteResponse(w, SplitterResponse{UnmarshalBodyErrMsg}, 400)
		return
	}

	if len(chopperReq.URI) == 0 {
		WriteResponse(w, SplitterResponse{URIParamMissingMsg}, 400)
		return
	}

	log.Debug("Processing splitter request", log.Data{"URI:": chopperReq.URI})
	awsReader, err := awsClient.GetFile(chopperReq.URI)
	if err != nil {
		WriteResponse(w, SplitterResponse{err.Error()}, 400)
		return
	}
	csvProcessor.Process(csv.NewReader(awsReader))
	WriteResponse(w, SplitterResponse{SuccessMsg}, 200)
}

// SetReader set the handler response reader
func SetReader(reader ByteSliceReader) {
	requestBodyReader = reader
}

// SetCSVProcessor set the CSV processor implementation.
func SetCSVProcessor(p splitter.CSVProcessor) {
	csvProcessor = p
}

// SetAWSClient set the AWS client implementation.
func SetAWSClient(c aws.AWSClient) {
	awsClient = c
}
