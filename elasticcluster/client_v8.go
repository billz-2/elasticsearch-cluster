package elasticcluster

import (
	"context"
	"errors"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

type clientV8 struct {
	*elasticsearch.Client
}

func NewClientV8(client *elasticsearch.Client) ESClient {
	return &clientV8{
		Client: client,
	}
}

func (c *clientV8) Search(ctx context.Context, req *SearchRequest) *Response {
	search := c.Client.Search
	opts := []func(*esapi.SearchRequest){
		search.WithContext(ctx),
	}
	if req.Index != "" {
		opts = append(opts, search.WithIndex(req.Index))
	}
	if req.Body != nil {
		opts = append(opts, search.WithBody(req.Body))
	}
	if req.From > 0 {
		opts = append(opts, search.WithFrom(req.From))
	}
	if req.Size > 0 {
		opts = append(opts, search.WithSize(req.Size))
	}
	if req.WithTrackTotalHits {
		opts = append(opts, search.WithTrackTotalHits(req.WithTrackTotalHits))
	}
	if req.WithPretty {
		opts = append(opts, search.WithPretty())
	}

	res, err := search(opts...)
	if err != nil {
		return &Response{Err: err}
	}

	data, err := ResponseDecode(res)
	if err != nil {
		return &Response{StatusCode: res.StatusCode, Err: err}
	}

	return &Response{StatusCode: res.StatusCode, Data: data}
}

func (c *clientV8) OpenPointInTime(ctx context.Context, req *OpenPointInTimeRequest) *Response {
	keepAlive := strings.TrimSpace(req.KeepAlive)
	if keepAlive == "" {
		keepAlive = "1m"
	}

	openPointInTime := c.Client.OpenPointInTime
	res, err := openPointInTime(
		getIndex(req.Index),
		keepAlive,
		openPointInTime.WithContext(ctx),
	)
	if err != nil {
		return &Response{Err: err}
	}

	data, err := ResponseDecode(res)
	if err != nil {
		return &Response{StatusCode: res.StatusCode, Err: err}
	}

	return &Response{StatusCode: res.StatusCode, Data: data}
}

func (c *clientV8) ClosePointInTime(ctx context.Context, req *ClosePointInTimeRequest) *Response {
	if req == nil || req.Body == nil {
		return &Response{Err: errors.New("close PIT body is required")}
	}
	closePointInTime := c.Client.ClosePointInTime
	res, err := closePointInTime(
		closePointInTime.WithContext(ctx),
		closePointInTime.WithBody(req.Body),
	)
	if err != nil {
		return &Response{Err: err}
	}

	data, err := ResponseDecode(res)
	if err != nil {
		return &Response{StatusCode: res.StatusCode, Err: err}
	}

	return &Response{StatusCode: res.StatusCode, Data: data}
}

func (c *clientV8) CreateIndex(ctx context.Context, req *CreateIndexRequest) *Response {
	createReq := esapi.IndicesCreateRequest{
		Index: req.Index,
		Body:  req.Body,
	}
	res, err := createReq.Do(ctx, c.Client)
	if err != nil {
		return &Response{Err: err}
	}

	data, err := ResponseDecode(res)
	if err != nil {
		return &Response{StatusCode: res.StatusCode, Err: err}
	}

	return &Response{StatusCode: res.StatusCode, Data: data}
}

func (c *clientV8) DeleteIndex(ctx context.Context, req *DeleteIndexRequest) *Response {
	deleteReq := esapi.IndicesDeleteRequest{
		Index: getIndex(req.Index),
	}
	res, err := deleteReq.Do(ctx, c.Client)
	if err != nil {
		return &Response{Err: err}
	}

	data, err := ResponseDecode(res)
	if err != nil {
		return &Response{StatusCode: res.StatusCode, Err: err}
	}

	return &Response{StatusCode: res.StatusCode, Data: data}
}

func (c *clientV8) Create(ctx context.Context, req *CreateRequest) *Response {
	createReq := esapi.CreateRequest{
		Index:      req.Index,
		DocumentID: req.DocumentID,
		Body:       req.Body,
	}

	res, err := createReq.Do(ctx, c.Client)
	if err != nil {
		return &Response{Err: err}
	}

	data, err := ResponseDecode(res)
	if err != nil {
		return &Response{StatusCode: res.StatusCode, Err: err}
	}

	return &Response{StatusCode: res.StatusCode, Data: data}
}
