package esclient

import (
	"context"
	"net/http"
	"net/url"

	elasticV8 "github.com/elastic/go-elasticsearch/v8"
	elasticV9 "github.com/elastic/go-elasticsearch/v9"
	"github.com/pkg/errors"
)

// ESClient is the core interface for Elasticsearch operations.
// It abstracts both v8 and v9 clients using HTTP transport layer.
type ESClient interface {
	Do(ctx context.Context, req *http.Request) (*http.Response, error)
}

// esAdapter adapts ES v8/v9 clients to unified ESClient interface.
type esAdapter struct {
	perform func(req *http.Request) (*http.Response, error)
	baseURL *url.URL
	log     Logger
}

// Do executes HTTP request with context, resolving relative URLs to absolute.
func (ea *esAdapter) Do(ctx context.Context, req *http.Request) (*http.Response, error) {
	if req.URL == nil {
		return nil, errors.New("request url is nil")
	}

	r := req.Clone(ctx)
	if !r.URL.IsAbs() {
		if ea.baseURL == nil {
			return nil, errors.New("base url is nil")
		}
		u := *ea.baseURL
		u.Path = r.URL.Path
		u.RawQuery = r.URL.RawQuery
		r.URL = &u
	}

	ea.log.DebugWithCtx(ctx, "elasticsearch request", map[string]interface{}{
		"method": r.Method,
		"path":   r.URL.Path,
		"host":   r.URL.Host,
	})

	resp, err := ea.perform(r)
	if err != nil {
		return nil, err
	}

	ea.log.DebugWithCtx(ctx, "elasticsearch response", map[string]interface{}{
		"status_code": resp.StatusCode,
		"path":        r.URL.Path,
	})

	return resp, nil
}

// NewESClientV8 creates ESClient from Elasticsearch v8 client.
func NewESClientV8(c *elasticV8.Client, baseURL *url.URL) ESClient {
	return NewESClientV8WithLogger(c, baseURL, nil)
}

// NewESClientV8WithLogger creates ESClient from Elasticsearch v8 client with logger.
func NewESClientV8WithLogger(c *elasticV8.Client, baseURL *url.URL, log Logger) ESClient {
	return &esAdapter{
		perform: c.Transport.Perform,
		baseURL: baseURL,
		log:     safeLogger(log),
	}
}

// NewESClientV9 creates ESClient from Elasticsearch v9 client.
func NewESClientV9(c *elasticV9.Client, baseURL *url.URL) ESClient {
	return NewESClientV9WithLogger(c, baseURL, nil)
}

// NewESClientV9WithLogger creates ESClient from Elasticsearch v9 client with logger.
func NewESClientV9WithLogger(c *elasticV9.Client, baseURL *url.URL, log Logger) ESClient {
	return &esAdapter{
		perform: c.Transport.Perform,
		baseURL: baseURL,
		log:     safeLogger(log),
	}
}
