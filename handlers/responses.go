package handlers

import (
	"encoding/json"
	"net/http"
)

func WriteResponse(w http.ResponseWriter, value interface{}, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(value)
}