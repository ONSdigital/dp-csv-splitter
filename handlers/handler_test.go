package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"bytes"
	"encoding/csv"
	"errors"
	. "github.com/smartystreets/goconvey/convey"
	"io"
	"io/ioutil"
	"sync"
)

var mutex = &sync.Mutex{}

// MockAWSCli mock implementation of aws.Client
type MockAWSCli struct {
	requestedFiles map[string]int
	fileBytes      []byte
	err            error
}

func newMockAwsClient() *MockAWSCli {
	mock := &MockAWSCli{requestedFiles: make(map[string]int)}
	SetAWSClient(mock)
	return mock
}

func (mock *MockAWSCli) GetFile(fileURI string) (io.Reader, error) {
	mutex.Lock()
	defer mutex.Unlock()

	if val, ok := mock.requestedFiles[fileURI]; ok {
		mock.requestedFiles[fileURI] = val + 1
	} else {
		mock.requestedFiles[fileURI] = 1
	}
	return bytes.NewReader(mock.fileBytes), mock.err
}

func (mock *MockAWSCli) getTotalInvocations() int {
	var count = 0
	for _, val := range mock.requestedFiles {
		count += val
	}
	return count
}

// MockCSVProcessor
type MockCSVProcessor struct {
	invocations int
}

func newMockCSVProcessor() *MockCSVProcessor {
	mock := &MockCSVProcessor{invocations: 0}
	SetCSVProcessor(mock)
	return mock
}

// Process mock implementation of the Process function.
func (p *MockCSVProcessor) Process(csvReader *csv.Reader) {
	mutex.Lock()
	defer mutex.Unlock()
	p.invocations++
}

func (mock *MockAWSCli) getInvocationsByURI(uri string) int {
	return mock.requestedFiles[uri]
}

func mockReader(r io.Reader) ([]byte, error) {
	return []byte{}, errors.New("BOB")
}

func TestHandler(t *testing.T) {
	Convey("Should return error response if no uri parameter is provided.", t, func() {
		recorder := httptest.NewRecorder()
		mockAWSCli, mockCSVProcessor := setMocks(mockReader)

		Handle(recorder, createRequest(nil))

		actual := extractResponseBody(recorder)

		So(*actual, ShouldResemble, SplitterErrorResponse(ReadRequestBodyErrMsg, 400))
		So(0, ShouldEqual, mockAWSCli.getTotalInvocations())
		So(0, ShouldEqual, mockCSVProcessor.invocations)
	})

	Convey("Should invoke AWSClient once with the request file URI.", t, func() {
		recorder := httptest.NewRecorder()
		mockAWSCli, mockCSVProcessor := setMocks(ioutil.ReadAll)

		fileLocation := "/aws"
		Handle(recorder, createRequest(SplitterRequest{fileLocation}))

		actual := extractResponseBody(recorder)

		So(*actual, ShouldResemble, SplitterSuccessResponse(SuccessMsg, 200))
		So(1, ShouldEqual, mockAWSCli.getTotalInvocations())
		So(1, ShouldEqual, mockAWSCli.getInvocationsByURI(fileLocation))
		So(1, ShouldEqual, mockCSVProcessor.invocations)
	})

	Convey("Should return appropriate error if cannot unmarshall the request body into a SplitterRequest.", t, func() {
		recorder := httptest.NewRecorder()
		mockAWSCli, mockCSVProcessor := setMocks(ioutil.ReadAll)

		Handle(recorder, createRequest("This is not a SplitterRequest"))

		actual := extractResponseBody(recorder)

		So(0, ShouldEqual, mockAWSCli.getTotalInvocations())
		So(0, ShouldEqual, mockCSVProcessor.invocations)
		So(*actual, ShouldResemble, SplitterErrorResponse(UnmarshalBodyErrMsg, 400))
	})

	Convey("Should return appropriate error if request body has empty of missing uri field.", t, func() {
		recorder := httptest.NewRecorder()
		request := createRequest(SplitterRequest{})

		mockAWSCli, mockCSVProcessor := setMocks(ioutil.ReadAll)

		Handle(recorder, request)

		actual := extractResponseBody(recorder)

		So(0, ShouldEqual, mockAWSCli.getTotalInvocations())
		So(0, ShouldEqual, mockCSVProcessor.invocations)
		So(*actual, ShouldResemble, SplitterErrorResponse(URIParamMissingMsg, 400))
	})

	Convey("Should return appropriate error if the awsClient returns an error.", t, func() {
		recorder := httptest.NewRecorder()
		uri := "/target.csv"
		awsErrMsg := "THIS IS AN AWS ERROR"

		mockAWSCli, mockCSVProcessor := setMocks(ioutil.ReadAll)
		mockAWSCli.err = errors.New(awsErrMsg)

		Handle(recorder, createRequest(SplitterRequest{URI: uri}))
		actual := extractResponseBody(recorder)

		So(1, ShouldEqual, mockAWSCli.getTotalInvocations())
		So(1, ShouldEqual, mockAWSCli.getInvocationsByURI(uri))
		So(0, ShouldEqual, mockCSVProcessor.invocations)
		So(*actual, ShouldResemble, SplitterErrorResponse(awsErrMsg, 400))
	})

	Convey("Should return success response for happy path scenario", t, func() {
		recorder := httptest.NewRecorder()
		uri := "/target.csv"

		mockAWSCli, mockCSVProcessor := setMocks(ioutil.ReadAll)

		Handle(recorder, createRequest(SplitterRequest{URI: uri}))
		actual := extractResponseBody(recorder)

		So(1, ShouldEqual, mockAWSCli.getTotalInvocations())
		So(1, ShouldEqual, mockAWSCli.getInvocationsByURI(uri))
		So(1, ShouldEqual, mockCSVProcessor.invocations)
		So(*actual, ShouldResemble, SplitterSuccessResponse(SuccessMsg, 200))
	})
}

func extractResponseBody(rec *httptest.ResponseRecorder) *SplitterResponse {
	var actual = &SplitterResponse{}
	json.Unmarshal([]byte(rec.Body.String()), actual)
	return actual
}

func createRequest(body interface{}) *http.Request {
	b, _ := json.Marshal(body)
	request, _ := http.NewRequest("POST", SplitterURI, bytes.NewBuffer(b))
	return request
}

func setMocks(reader ByteSliceReader) (*MockAWSCli, *MockCSVProcessor) {
	mockAWSCli := newMockAwsClient()
	mockCSVProcessor := newMockCSVProcessor()
	SetReader(reader)
	return mockAWSCli, mockCSVProcessor
}
