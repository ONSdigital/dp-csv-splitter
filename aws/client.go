package aws

import "github.com/ONSdigital/go-ns/log"

type AWSClient interface {
	GetFile(fileURI string)
}

type Client struct {}

func NewClient() AWSClient {
	return &Client{}
}

func (cli *Client) GetFile(fileURI string) {
	log.Debug("Getting file from AWS", log.Data{
		"fileURI": fileURI,
	})
}


