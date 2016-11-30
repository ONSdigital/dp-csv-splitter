package handlers

import (
	"encoding/json"
	"net/http"
)

// SplitterErrorResponse function creates a new SplitterResponse with an error message and status code.
func SplitterErrorResponse(message string, status int) SplitterResponse {
	return SplitterResponse{Error: message, Status: status}
}

// SplitterSuccessResponse function creates a new SplitterResponse with an success message and status code.
func SplitterSuccessResponse(message string, status int) SplitterResponse {
	return SplitterResponse{Message: message, Status: status}
}

// SplitterResponse struct defines the response for the /splitter API.
type SplitterResponse struct {
	Error   string `json:"error,omitempty"`
	Message string `json:"message,omitempty"`
	Status  int    `json:"status"`
}

// writeErrorResponse write the Response
func (r SplitterResponse) writeResponse(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(r.Status)
	json.NewEncoder(w).Encode(r)
}

// SplitterRequest struct defines a splitter request
type SplitterRequest struct {
	URI string `json:"uri"`
}
