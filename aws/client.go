package aws

import (
	"bytes"
	"github.com/ONSdigital/dp-csv-splitter/config"
	"github.com/ONSdigital/go-ns/log"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"io"
	"io/ioutil"
)

// AWSClient interface defining the AWS client.
type AWSClient interface {
	GetFile(fileURI string) (io.Reader, error)
}

// Client AWS client implementation.
type Client struct{}

// NewClient create new AWSClient.
func NewClient() AWSClient {
	return &Client{}
}

// GetFile get the requested file from AWS.
func (cli *Client) GetFile(fileURI string) (io.Reader, error) {
	log.Debug("Getting file from AWS", log.Data{
		"fileURI": fileURI,
	})

	session, err := session.NewSession(&aws.Config{
		Region: aws.String(config.AWSRegion),
	})

	if err != nil {
		log.Error(err, nil)
		return nil, err
	}

	s3Service := s3.New(session)
	request := &s3.GetObjectInput{}
	request.SetBucket(config.S3Bucket)
	request.SetKey(fileURI)

	result, err := s3Service.GetObject(request)

	if err != nil {
		log.Error(err, nil)
		return nil, err
	}

	b, err := ioutil.ReadAll(result.Body)
	defer result.Body.Close()

	if err != nil {
		log.Error(err, nil)
		return nil, err
	}

	return bytes.NewReader(b), nil
}
