package esclient

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"

	"github.com/pkg/errors"
)

// doJSON executes HTTP request and decodes JSON response.
// Returns status code and error if any.
func doJSON(ctx context.Context, c ESClient, req *http.Request, out interface{}) (int, error) {
	res, err := c.Do(ctx, req)
	if err != nil {
		return 0, errors.Wrap(err, "http request failed")
	}
	defer res.Body.Close() //nolint:errcheck

	status := res.StatusCode

	if out == nil {
		return status, nil
	}

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return status, errors.Wrap(err, "failed to read response body")
	}

	if err := json.Unmarshal(bodyBytes, out); err != nil {
		return status, errors.Wrapf(err, "failed to decode JSON response (status %d)", status)
	}

	return status, nil
}

// newURL creates absolute URL from base URL, path and query parameters.
func newURL(base *url.URL, path string, q url.Values) *url.URL {
	u := *base
	u.Path = path
	if q != nil {
		u.RawQuery = q.Encode()
	}
	return &u
}

// jsonBody marshals value to JSON and returns io.Reader.
func jsonBody(v interface{}) (io.Reader, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal JSON")
	}
	return bytes.NewReader(b), nil
}

// contentTypeJSON sets Content-Type header to application/json.
func contentTypeJSON(req *http.Request) {
	req.Header.Set("Content-Type", "application/json")
}

// parseBaseURL parses and validates base URL.
func parseBaseURL(address string) (*url.URL, error) {
	if address == "" {
		return nil, errors.New("empty base URL")
	}

	u, err := url.Parse(address)
	if err != nil {
		return nil, errors.Wrap(err, "invalid base URL")
	}

	if u.Scheme == "" || u.Host == "" {
		return nil, errors.New("base URL must be absolute (include scheme and host)")
	}

	return u, nil
}
