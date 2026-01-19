package esclient

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

// Client provides typed Elasticsearch operations on top of ESClient.
type Client struct {
	es      ESClient
	baseURL *url.URL
	log     Logger
}

// NewClient creates a typed client wrapper around ESClient.
func NewClient(es ESClient, baseURL string) (*Client, error) {
	return NewClientWithLogger(es, baseURL, nil)
}

// NewClientWithLogger creates a typed client wrapper around ESClient with logger.
func NewClientWithLogger(es ESClient, baseURL string, log Logger) (*Client, error) {
	u, err := parseBaseURL(baseURL)
	if err != nil {
		return nil, err
	}

	return &Client{
		es:      es,
		baseURL: u,
		log:     safeLogger(log),
	}, nil
}

// Search performs search request.
func (c *Client) Search(ctx context.Context, req *SearchRequest) (*SearchResponse, error) {
	if req.Index == "" {
		return nil, errors.New("index name is required")
	}

	target := DetectIndexTarget(req.Index)
	queryCopy := deepCopyMap(req.Query)

	if target == IndexTargetShared {
		mutator := NewQueryMutator()
		if err := mutator.InjectCompanyFilter(queryCopy, req.CompanyID, target); err != nil {
			return nil, errors.Wrap(err, "failed to inject company filter")
		}
	}

	body, err := jsonBody(queryCopy)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal query")
	}

	path := fmt.Sprintf("/%s/_search", req.Index)
	query := url.Values{}

	if req.Size != nil {
		query.Set("size", strconv.Itoa(*req.Size))
	}
	if req.From != nil {
		query.Set("from", strconv.Itoa(*req.From))
	}
	if req.WithTrackTotalHits {
		query.Set("track_total_hits", "true")
	}

	u := newURL(c.baseURL, path, query)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), body)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create search request")
	}
	contentTypeJSON(httpReq)

	var resp SearchResponse
	status, err := doJSON(ctx, c.es, httpReq, &resp, c.log)
	if err != nil {
		return nil, err
	}

	if status != http.StatusOK {
		return nil, &StatusError{Op: "search", StatusCode: status}
	}

	return &resp, nil
}

// OpenPIT opens point-in-time for pagination.
func (c *Client) OpenPIT(ctx context.Context, req *OpenPITRequest) (*PIT, error) {
	if req.Index == "" {
		return nil, errors.New("index name is required")
	}
	if req.KeepAlive == "" {
		req.KeepAlive = "1m"
	}

	path := fmt.Sprintf("/%s/_pit", req.Index)
	query := url.Values{}
	query.Set("keep_alive", req.KeepAlive)

	u := newURL(c.baseURL, path, query)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create open PIT request")
	}

	var pit PIT
	status, err := doJSON(ctx, c.es, httpReq, &pit, c.log)
	if err != nil {
		return nil, err
	}

	if status != http.StatusOK {
		return nil, &StatusError{Op: "open_pit", StatusCode: status}
	}

	return &pit, nil
}

// ClosePIT closes point-in-time.
func (c *Client) ClosePIT(ctx context.Context, pitID string) error {
	if pitID == "" {
		return errors.New("PIT ID is required")
	}

	path := "/_pit"
	body, err := jsonBody(map[string]interface{}{
		"id": pitID,
	})
	if err != nil {
		return err
	}

	u := newURL(c.baseURL, path, nil)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodDelete, u.String(), body)
	if err != nil {
		return errors.Wrap(err, "failed to create close PIT request")
	}
	contentTypeJSON(httpReq)

	status, err := doJSON(ctx, c.es, httpReq, nil, c.log)
	if err != nil {
		return err
	}

	if status != http.StatusOK {
		return &StatusError{Op: "close_pit", StatusCode: status}
	}

	return nil
}

// Bulk performs bulk operations.
func (c *Client) Bulk(ctx context.Context, req *BulkRequest) (*BulkResponse, error) {
	path := "/_bulk"
	if req.Index != "" {
		path = fmt.Sprintf("/%s/_bulk", req.Index)
	}

	query := url.Values{
		"refresh": []string{"wait_for"},
	}
	u := newURL(c.baseURL, path, query)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), req.Body)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create bulk request")
	}
	httpReq.Header.Set("Content-Type", "application/x-ndjson")

	var resp BulkResponse
	status, err := doJSON(ctx, c.es, httpReq, &resp, c.log)
	if err != nil {
		return nil, err
	}

	if status != http.StatusOK {
		return nil, &StatusError{Op: "bulk", StatusCode: status}
	}
	return &resp, nil
}

// DeleteByQuery deletes documents matching query.
func (c *Client) DeleteByQuery(ctx context.Context, req *DeleteByQueryRequest) (*DeleteByQueryResponse, error) {
	if req.Index == "" {
		return nil, errors.New("index name is required")
	}

	target := DetectIndexTarget(req.Index)
	queryCopy := deepCopyMap(req.Query)

	if target == IndexTargetShared {
		mutator := NewQueryMutator()
		if err := mutator.InjectCompanyFilter(queryCopy, req.CompanyID, target); err != nil {
			return nil, errors.Wrap(err, "failed to inject company filter")
		}
	}

	body, err := jsonBody(queryCopy)
	if err != nil {
		return nil, errors.Wrap(err, "failed to encode query")
	}

	path := fmt.Sprintf("/%s/_delete_by_query", req.Index)
	u := newURL(c.baseURL, path, nil)

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), body)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create delete by query request")
	}
	contentTypeJSON(httpReq)

	var resp DeleteByQueryResponse
	status, err := doJSON(ctx, c.es, httpReq, &resp, c.log)
	if err != nil {
		return nil, err
	}

	if status != http.StatusOK {
		return nil, &StatusError{Op: "delete_by_query", StatusCode: status}
	}

	return &resp, nil
}

// CreateIndex creates a new index with mappings and settings.
func (c *Client) CreateIndex(ctx context.Context, req *CreateIndexRequest) error {
	if req.Index == "" {
		return errors.New("index name is required")
	}

	path := fmt.Sprintf("/%s", req.Index)
	u := newURL(c.baseURL, path, nil)

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPut, u.String(), req.Body)
	if err != nil {
		return errors.Wrap(err, "failed to create index request")
	}
	contentTypeJSON(httpReq)

	status, err := doJSON(ctx, c.es, httpReq, nil, c.log)
	if err != nil {
		return err
	}

	if status != http.StatusOK && status != http.StatusCreated {
		return &StatusError{Op: "create_index", StatusCode: status}
	}

	return nil
}

// DeleteIndex deletes an index.
func (c *Client) DeleteIndex(ctx context.Context, indexName string) error {
	if indexName == "" {
		return errors.New("index name is required")
	}

	path := fmt.Sprintf("/%s", indexName)
	u := newURL(c.baseURL, path, nil)

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodDelete, u.String(), nil)
	if err != nil {
		return errors.Wrap(err, "failed to create delete index request")
	}

	status, err := doJSON(ctx, c.es, httpReq, nil, c.log)
	if err != nil {
		return err
	}

	if status != http.StatusOK {
		return &StatusError{Op: "delete_index", StatusCode: status}
	}

	return nil
}

// IndexExists checks if index exists.
func (c *Client) IndexExists(ctx context.Context, indexName string) (bool, error) {
	if indexName == "" {
		return false, errors.New("index name is required")
	}

	path := fmt.Sprintf("/%s", indexName)
	u := newURL(c.baseURL, path, nil)

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodHead, u.String(), nil)
	if err != nil {
		return false, errors.Wrap(err, "failed to create index exists request")
	}

	status, err := doJSON(ctx, c.es, httpReq, nil, c.log)
	if err != nil {
		return false, err
	}

	return status == http.StatusOK, nil
}

// Count counts documents matching query.
func (c *Client) Count(ctx context.Context, req *CountRequest) (*CountResponse, error) {
	if req.Index == "" {
		return nil, errors.New("index name is required")
	}

	target := DetectIndexTarget(req.Index)
	query := req.Query
	if query == nil {
		query = make(map[string]any)
	}
	queryCopy := deepCopyMap(query)

	if target == IndexTargetShared {
		mutator := NewQueryMutator()
		if err := mutator.InjectCompanyFilter(queryCopy, req.CompanyID, target); err != nil {
			return nil, errors.Wrap(err, "failed to inject company filter")
		}
	}

	body, err := jsonBody(queryCopy)
	if err != nil {
		return nil, errors.Wrap(err, "failed to encode query")
	}

	path := fmt.Sprintf("/%s/_count", req.Index)
	u := newURL(c.baseURL, path, nil)

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), body)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create count request")
	}
	if body != nil {
		contentTypeJSON(httpReq)
	}

	var resp CountResponse
	status, err := doJSON(ctx, c.es, httpReq, &resp, c.log)
	if err != nil {
		return nil, err
	}

	if status != http.StatusOK {
		return nil, &StatusError{Op: "count", StatusCode: status}
	}

	return &resp, nil
}

// UpdateByQuery updates documents matching query.
func (c *Client) UpdateByQuery(ctx context.Context, req *UpdateByQueryRequest) (*UpdateByQueryResponse, error) {
	if req.Index == "" {
		return nil, errors.New("index name is required")
	}

	target := DetectIndexTarget(req.Index)
	queryCopy := deepCopyMap(req.Query)

	if target == IndexTargetShared {
		mutator := NewQueryMutator()
		if err := mutator.InjectCompanyFilter(queryCopy, req.CompanyID, target); err != nil {
			return nil, errors.Wrap(err, "failed to inject company filter")
		}
	}

	body, err := jsonBody(queryCopy)
	if err != nil {
		return nil, errors.Wrap(err, "failed to encode query")
	}

	path := fmt.Sprintf("/%s/_update_by_query", req.Index)
	u := newURL(c.baseURL, path, nil)

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), body)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create update by query request")
	}
	contentTypeJSON(httpReq)

	var resp UpdateByQueryResponse
	status, err := doJSON(ctx, c.es, httpReq, &resp, c.log)
	if err != nil {
		return nil, err
	}

	if status != http.StatusOK {
		return nil, &StatusError{Op: "update_by_query", StatusCode: status}
	}

	return &resp, nil
}

// CreateDocument creates or updates a document with specific ID.
func (c *Client) CreateDocument(ctx context.Context, req *CreateDocumentRequest) (*CreateDocumentResponse, error) {
	if req.Index == "" {
		return nil, errors.New("index name is required")
	}
	if req.DocumentID == "" {
		return nil, errors.New("document ID is required")
	}

	path := fmt.Sprintf("/%s/_doc/%s", req.Index, req.DocumentID)
	u := newURL(c.baseURL, path, nil)

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPut, u.String(), req.Body)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create document request")
	}
	contentTypeJSON(httpReq)

	var resp CreateDocumentResponse
	status, err := doJSON(ctx, c.es, httpReq, &resp, c.log)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK && status != http.StatusCreated {
		return nil, &StatusError{Op: "create_document", StatusCode: status}
	}

	return &resp, nil
}

// RawRequest executes raw HTTP request (for custom operations).
func (c *Client) RawRequest(ctx context.Context, method, path string, body interface{}) (int, map[string]interface{}, error) {
	var bodyReader interface{}
	if body != nil {
		r, err := jsonBody(body)
		if err != nil {
			return 0, nil, err
		}
		bodyReader = r
	}

	u := newURL(c.baseURL, path, nil)
	httpReq, err := http.NewRequestWithContext(ctx, strings.ToUpper(method), u.String(), bodyReader.(interface{ Read([]byte) (int, error) }))
	if err != nil {
		return 0, nil, errors.Wrap(err, "failed to create raw request")
	}

	if body != nil {
		contentTypeJSON(httpReq)
	}

	var result map[string]interface{}
	status, err := doJSON(ctx, c.es, httpReq, &result, c.log)

	return status, result, err
}
