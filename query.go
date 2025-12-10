package elasticcluster

import (
	"bytes"
	"encoding/json"
)

func EncodeQuery(query interface{}) (*bytes.Buffer, error) {
	var buff bytes.Buffer
	if err := json.NewEncoder(&buff).Encode(query); err != nil {
		return nil, err
	}

	return &buff, nil
}
