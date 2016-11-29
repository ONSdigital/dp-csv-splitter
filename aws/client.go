package aws

import (
	"github.com/ONSdigital/go-ns/log"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"io/ioutil"
	"github.com/aws/aws-sdk-go/aws"
	"bytes"
	"io"
)

const awsBucketName = "dp-csv-splitter-1"
const awsRegion = "eu-west-1"

type AWSClient interface {
	GetFile(fileURI string) (io.Reader, error)
}

type Client struct{}

func NewClient() AWSClient {
	return &Client{}
}

func (cli *Client) GetFile(fileURI string) (io.Reader, error) {
	log.Debug("Getting file from AWS", log.Data{
		"fileURI": fileURI,
	})

	session, err := session.NewSession(&aws.Config{
		Region: aws.String(awsRegion),
	})

	if err != nil {
		log.Error(err, nil)
		return nil, err
	}

	s3Service := s3.New(session)
	request := &s3.GetObjectInput{}
	request.SetBucket(awsBucketName)
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


