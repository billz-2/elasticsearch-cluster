package elasticcluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/elastic/go-elasticsearch/v8/esapi"
	esapiv9 "github.com/elastic/go-elasticsearch/v9/esapi"
)

type Response struct {
	StatusCode int
	Err        error
	Data       map[string]interface{}
}

// ErrNoClient is returned when no ES client could be resolved.
var ErrNoClient = errors.New("no elasticsearch client available")

// ErrInvalidIndexType is returned when the index type is invalid
var ErrInvalidIndexType = errors.New("invalid index type")

func ResponseDecode(r *esapi.Response) (map[string]interface{}, error) {
	res, err := handleESResponseBody(r.Body, r.StatusCode, r.IsError())
	if err != nil {
		return nil, err
	}

	return res, nil
}

func ResponseDecodeV9(r *esapiv9.Response) (map[string]interface{}, error) {
	res, err := handleESResponseBody(r.Body, r.StatusCode, r.IsError())
	if err != nil {
		return nil, err
	}

	return res, nil
}

func handleESResponseBody(body io.ReadCloser, statusCode int, isError bool) (map[string]interface{}, error) {
	if isError {
		var e map[string]interface{}
		err := json.NewDecoder(body).Decode(&e)
		if err != nil {
			return nil, err
		}
		var errorStr string
		switch v := e["error"].(type) {
		case nil:
			errorStr = "nil"
		case string:
			errorStr = v
		case map[string]interface{}:
			errorStr = fmt.Sprintf("ResponseError: [%d] %s: %s",
				statusCode,
				v["type"],
				v["reason"])
		}
		err = errors.New(errorStr)

		return nil, err
	}
	defer body.Close()
	var res map[string]interface{}
	if err := json.NewDecoder(body).Decode(&res); err != nil {
		return nil, err
	}

	return res, nil
}

func (r *Response) IsError() bool {
	return r.StatusCode > 299
}
