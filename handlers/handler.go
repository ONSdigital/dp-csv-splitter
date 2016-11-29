package handlers

import (
	"net/http"
	"github.com/ONSdigital/dp-csv-splitter/aws"
	"io/ioutil"
	"encoding/json"
	"fmt"
	"io"
)

// URIParamMissingMsg error message for URI parameter missing from request parameters
const URIParamMissingMsg = "Please specify a uri parameter."
const URIParamName = "uri"
const SuccessMsg = "Your file has been sent to the Chopper"
const ReadRequestBodyErrMsg = "Could not read body"

var AWSCli aws.AWSClient

type ByteSliceReader func(r io.Reader) ([]byte, error)

var requestBodyReader ByteSliceReader = ioutil.ReadAll


// Handle TODO
func Handle(w http.ResponseWriter, req *http.Request) {
	bytes, err := requestBodyReader(req.Body)
	if err != nil {
		SplitterErrorResponse(ReadRequestBodyErrMsg, 400).writeErrorResponse(w)
		return
	}

	var chopperReq ChopperRequest
	if err := json.Unmarshal(bytes, &chopperReq); err != nil {
		SplitterErrorResponse("Could not unmarshal body", 400).writeErrorResponse(w)
		return
	}


	if len(chopperReq.URI) == 0 {
		fmt.Printf("URI EMPTY", chopperReq)
		SplitterErrorResponse(URIParamMissingMsg, 400).writeErrorResponse(w)
		return
	}

	_, err = AWSCli.GetFile(chopperReq.URI)
	if err != nil {
		SplitterErrorResponse("Failed with some AWS stuff", 400)
	}

	SplitterSuccessResponse(SuccessMsg, 200).writeErrorResponse(w)
}

func SetReader(reader ByteSliceReader) {
	requestBodyReader = reader
}