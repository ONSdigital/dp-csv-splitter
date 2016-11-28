package handlers

import (
	"encoding/json"
	"net/http"
)

func SplitterErrorResponse(message string, status int) SplitterResponse {
	return SplitterResponse{Error: message, Status: status}
}

func SplitterSuccessResponse(message string, status int) SplitterResponse {
	return SplitterResponse{Message: message, Status: status}
}

// Response
type SplitterResponse struct {
	Error   string `json:"error,omitempty"`
	Message string `json:"message,omitempty"`
	Status  int    `json:"status"`
}

// writeErrorResponse write the Response to the re
func (r SplitterResponse) writeErrorResponse(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(r.Status)
	json.NewEncoder(w).Encode(r)
}

// TODO should rename thing to something appropriate
type ChopperRequest struct {
	URI string `json:"uri"`
}
