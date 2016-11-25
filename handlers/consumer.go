package handlers

import (
	"net/http"
	"encoding/json"
)

func ErrorResponse(message string, status int) Response {
	return Response{Error: message, Status: status}
}

func SuccessResponse(message string, status int) Response {
	return Response{Message: message, Status: status}
}

type Response struct {
	Error   string `json:"error,omitempty"`
	Message string `json:"message,omitempty"`
	Status  int `json:"status"`
}

func (r Response) writeErrorResponse(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(r.Status)
	json.NewEncoder(w).Encode(r)
}

// Handle TODO
func Handle(w http.ResponseWriter, req *http.Request) {
	fileLocation := req.URL.Query().Get("uri")
	if len(fileLocation) == 0 {
		ErrorResponse("Please specify a uri parameter.", 400).writeErrorResponse(w)
		return
	}
	SuccessResponse("Get to da Choppa!", 200).writeErrorResponse(w)
}
