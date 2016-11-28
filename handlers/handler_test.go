package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"sync"
	"bytes"
	"fmt"
	"io"
	"errors"
	"io/ioutil"
)

var mutex = &sync.Mutex{}

type MockAWSCli struct {
	requestedFiles map[string]int
}

func newMockAwsClient() *MockAWSCli {
	return &MockAWSCli{requestedFiles: make(map[string] int)}
}

func (mock *MockAWSCli) GetFile(fileURI string) {
	mutex.Lock()
	defer mutex.Unlock()

	if val, ok := mock.requestedFiles[fileURI]; ok {
		mock.requestedFiles[fileURI] = val + 1
	} else {
		fmt.Println("inside else")
		mock.requestedFiles[fileURI] = 1
	}
}

func (mock *MockAWSCli) getTotalInvocations() int {
	var count = 0
	for _, val := range mock.requestedFiles {
		count += val
	}
	return count
}

func (mock *MockAWSCli) getInvocationsByURI(uri string) int {
	return mock.requestedFiles[uri]
}

func mockReader(r io.Reader) ([]byte, error) {
	return []byte{}, errors.New("BOB")
}

func TestHandler(t *testing.T) {
	Convey("Should return error response if no uri parameter is provided.", t, func() {

		SetReader(mockReader)

		recorder := httptest.NewRecorder()
		request, _ := http.NewRequest("POST", "/chopper", nil)

		mockAWSCli := newMockAwsClient()
		AWSCli = mockAWSCli

		Handle(recorder, request)

		actual := &SplitterResponse{}
		json.Unmarshal([]byte(recorder.Body.String()), actual)

		So(*actual, ShouldResemble, SplitterErrorResponse(ReadRequestBodyErrMsg, 400))
		So(0, ShouldEqual, mockAWSCli.getTotalInvocations())
	})

	Convey("Should invoke AWSClient once with the request file URI.", t, func() {
		recorder := httptest.NewRecorder()
		fileLocation := "/aws"
		b, _ := json.Marshal(ChopperRequest{fileLocation})
		request, _ := http.NewRequest("GET", "/chopper", bytes.NewBuffer(b))

		mockAWSCli := newMockAwsClient()
		AWSCli = mockAWSCli

		SetReader(ioutil.ReadAll)
		Handle(recorder, request)

		var actual = &SplitterResponse{}
		json.Unmarshal([]byte(recorder.Body.String()), actual)

		So(*actual, ShouldResemble, SplitterSuccessResponse(SuccessMsg, 200))
		So(1, ShouldEqual, mockAWSCli.getTotalInvocations())
		So(1, ShouldEqual, mockAWSCli.getInvocationsByURI(fileLocation))
	})
}
