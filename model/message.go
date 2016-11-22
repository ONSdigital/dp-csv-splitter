package model

type Message struct {
	Index int     `json:"index"`
	Row []string  `json:"row"`
}