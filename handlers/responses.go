package handlers

import (
	"encoding/json"
	"net/http"
)

// SplitterResponse struct defines the response for the /splitter API.
type SplitterResponse struct {
	Message string `json:"message,omitempty"`
}

// SplitterRequest struct defines a splitter request
type SplitterRequest struct {
	URI string `json:"uri"`
}


func WriteResponse(w http.ResponseWriter, value interface{}, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(value)
}