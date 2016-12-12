package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"bytes"
	"errors"
	. "github.com/smartystreets/goconvey/convey"
	"io"
	"io/ioutil"
	"sync"
	"time"
)

var mutex = &sync.Mutex{}

// MockAWSCli mock implementation of aws.Client
type MockAWSCli struct {
	invocationParameters []string
	fileBytes            []byte
	err                  error
}

func (mock *MockAWSCli) GetCSV(fileURI string) (io.Reader, error) {
	mutex.Lock()
	defer mutex.Unlock()
	mock.invocationParameters = append(mock.invocationParameters, fileURI)
	return bytes.NewReader(mock.fileBytes), mock.err
}

// MockCSVProcessor
type MockCSVProcessor struct {
	invocations int
}

func newMockCSVProcessor() *MockCSVProcessor {
	mock := &MockCSVProcessor{invocations: 0}
	setCSVProcessor(mock)
	return mock
}

// Process mock implementation of the Process function.
func (p *MockCSVProcessor) Process(r io.Reader, filename string, startTime time.Time, datasetId string) {
	mutex.Lock()
	defer mutex.Unlock()
	p.invocations++
}

func TestSplitterHandler(t *testing.T) {
	var recorder *httptest.ResponseRecorder
	var request *http.Request

	Convey("Given a valid SplitterRequest", t, func() {
		recorder := httptest.NewRecorder()
		uri := "/target.csv"
		request = createRequest(SplitterRequest{FilePath: uri})
		mockAWSCli, mockCSVProcessor := setMocks(ioutil.ReadAll)

		Convey("When the Splitter handler is invoked.", func() {
			Handle(recorder, request)
			splitterResponse, status := extractResponseBody(recorder)

			Convey("Then the AWSService is invoked 1 time", func() {
				So(1, ShouldEqual, len(mockAWSCli.invocationParameters))
			})

			Convey("And the AWSService is invoked with the expected FilePath parameter.", func() {
				So(uri, ShouldEqual, mockAWSCli.invocationParameters[0])
			})

			Convey("And the CSVProcessor is invoked 1 time.", func() {
				So(1, ShouldEqual, mockCSVProcessor.invocations)
			})

			Convey("And the Splitter response states the request was successful.", func() {
				So(splitterResponse, ShouldResemble, splitterResponseSuccess)
			})

			Convey("And the http reponse status code is OK.", func() {
				So(status, ShouldResemble, http.StatusOK)
			})
		})
	})

	Convey("Given a SplitterRequest with no filePath set.", t, func() {
		mockAWSCli, mockCSVProcessor := setMocks(ioutil.ReadAll)
		recorder = httptest.NewRecorder()
		request = createRequest(nil)

		Convey("When the Splitter handler is invoked.", func() {
			Handle(recorder, createRequest(nil))
			splitterResponse, status := extractResponseBody(recorder)

			Convey("Then the http reponse status code is Bad Request.", func() {
				So(status, ShouldResemble, http.StatusBadRequest)
			})

			Convey("And the SplitterResponse messgae states that no filePath has been provided.", func() {
				So(splitterResponse, ShouldResemble, splitterRespFilePathMissing)
			})

			Convey("And the AWSService has zero invocations.", func() {
				So(0, ShouldEqual, len(mockAWSCli.invocationParameters))
			})

			Convey("And the CSV processor has zero invocations.", func() {
				So(0, ShouldEqual, mockCSVProcessor.invocations)
			})
		})
	})

	Convey("Given a request body that is not a SplitterRequest.", t, func() {
		mockAWSCli, mockCSVProcessor := setMocks(ioutil.ReadAll)
		recorder = httptest.NewRecorder()
		request = createRequest("Hello World")

		Convey("When the Splitter handler is invoked", func() {
			Handle(recorder, request)
			splitterResponse, status := extractResponseBody(recorder)

			Convey("Then the http reponse status code is Bad Request.", func() {
				So(status, ShouldResemble, http.StatusBadRequest)
			})

			Convey("And the SplitterResponse messgae states that the request body is invalid.", func() {
				So(splitterResponse, ShouldResemble, splitterRespUnmarshalBody)
			})

			Convey("And the AWSService has zero invocations", func() {
				So(0, ShouldEqual, len(mockAWSCli.invocationParameters))
			})

			Convey("And the CSV processor has zero invocations", func() {
				So(0, ShouldEqual, mockCSVProcessor.invocations)
			})
		})
	})

	Convey("Given a valid SplitterRequest which causes the AWSService to return an error.", t, func() {
		mockAWSCli, mockCSVProcessor := setMocks(ioutil.ReadAll)
		recorder = httptest.NewRecorder()
		uri := "/target.csv"
		awsErrMsg := "borked!"
		mockAWSCli.err = errors.New(awsErrMsg)
		request = createRequest(&SplitterRequest{FilePath: uri})

		Convey("When the Splitter handler is invocked.", func() {
			Handle(recorder, request)
			splitterResponse, status := extractResponseBody(recorder)

			Convey("Then the AWSService is invoked 1 time.", func() {
				So(1, ShouldEqual, len(mockAWSCli.invocationParameters))
			})

			Convey("And the AWSService is invoked with the expected FilePath parameter.", func() {
				So(uri, ShouldEqual, mockAWSCli.invocationParameters[0])
			})

			Convey("And the http response status is Bad Request.", func() {
				So(status, ShouldResemble, http.StatusBadRequest)
			})

			Convey("And the SplitterResponse message should contain the AWSService error", func() {
				So(splitterResponse, ShouldResemble, SplitterResponse{awsErrMsg})
			})

			Convey("And the CSVProcessor has zero invocations.", func() {
				So(0, ShouldEqual, mockCSVProcessor.invocations)
			})
		})
	})

	Convey("Given a SplitterRequest with a FilePath parameter of an unsupported file type.", t, func() {
		mockAWSCli, mockCSVProcessor := setMocks(ioutil.ReadAll)
		recorder = httptest.NewRecorder()
		uri := "/target.xsl"
		request = createRequest(&SplitterRequest{FilePath: uri})

		Convey("When the Splitter hanlder is invoked", func() {
			Handle(recorder, request)
			splitterResponse, status := extractResponseBody(recorder)

			Convey("Then the AWSServer has zero invocations", func() {
				So(0, ShouldEqual, len(mockAWSCli.invocationParameters))
			})

			Convey("And the CSVProcessor has zero invocations", func() {
				So(0, ShouldEqual, mockCSVProcessor.invocations)
			})

			Convey("And the SplitterResponse message states the filePath parameter is for an unsupported file type.", func() {
				So(splitterResponse, ShouldResemble, splitterRespUnsupportedFileType)
			})

			Convey("And the http response status is Bad Request.", func() {
				So(status, ShouldResemble, http.StatusBadRequest)
			})
		})
	})

	Convey("Given a SplitterRequest with an unreadable request body", t, func() {
		mockBodyReader := func(r io.Reader) ([]byte, error) {
			return []byte{}, errors.New("Body read error.")
		}

		mockAWSCli, mockCSVProcessor := setMocks(mockBodyReader)
		recorder = httptest.NewRecorder()
		request = createRequest(nil)

		Convey("When the Splitter handler is invoked", func() {
			Handle(recorder, request)
			splitterResponse, status := extractResponseBody(recorder)

			Convey("Then the AWSService has zero invocations", func() {
				So(0, ShouldEqual, len(mockAWSCli.invocationParameters))
			})

			Convey("And the CSVProcessor has zero invocations", func() {
				So(0, ShouldEqual, mockCSVProcessor.invocations)
			})

			Convey("And the SplitterResponse message states request body could not be read.", func() {
				So(splitterResponse, ShouldResemble, splitterRespReadReqBodyErr)
			})

			Convey("And the http response status is Bad Request.", func() {
				So(status, ShouldResemble, http.StatusBadRequest)
			})
		})
	})
}

func extractResponseBody(rec *httptest.ResponseRecorder) (SplitterResponse, int) {
	var actual = &SplitterResponse{}
	json.Unmarshal([]byte(rec.Body.String()), actual)
	return *actual, rec.Code
}

func createRequest(body interface{}) *http.Request {
	b, _ := json.Marshal(body)
	request, _ := http.NewRequest("POST", "/splitter", bytes.NewBuffer(b))
	return request
}

func setMocks(reader requestBodyReader) (*MockAWSCli, *MockCSVProcessor) {
	mockAWSCli := &MockAWSCli{invocationParameters: make([]string, 0)}
	setAWSService(mockAWSCli)

	mockCSVProcessor := newMockCSVProcessor()
	setReader(reader)
	return mockAWSCli, mockCSVProcessor
}