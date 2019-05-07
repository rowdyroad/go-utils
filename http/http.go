package http

import (
	"encoding/json"
	"io"
	"net/http"
)

func Response(w http.ResponseWriter, code int, data interface{}) {
	var response []byte
	var err error
	if data != nil {
		response, err = json.Marshal(data)
		if err != nil {
			Error(w, "Internal Error", http.StatusInternalServerError)
			return
		}
	}
	w.WriteHeader(code)
	if response != nil {
		w.Write(response)
	}
}

func ParseJSON(w http.ResponseWriter, r *http.Request, data interface{}) error {
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(data); err != nil && err != io.EOF {
		Error(w, "Unprocessable Entity", http.StatusUnprocessableEntity)
		return err
	}
	return nil
}

func FieldsError(w http.ResponseWriter, err map[string]string) {
	w.WriteHeader(http.StatusUnprocessableEntity)
	encoder := json.NewEncoder(w)
	encoder.Encode(struct {
		Error  string            `json:"error"`
		Fields map[string]string `json:"fields"`
	}{"validate error", err})
}

func Error(w http.ResponseWriter, err string, code int) {
	w.WriteHeader(code)
	encoder := json.NewEncoder(w)
	encoder.Encode(struct {
		Error string `json:"error"`
	}{err})
}
