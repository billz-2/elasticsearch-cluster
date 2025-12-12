package elasticcluster

import (
	"context"
	"errors"
	"strings"

	"github.com/elastic/go-elasticsearch/v9"
	esapiv9 "github.com/elastic/go-elasticsearch/v9/esapi"
)

type clientV9 struct {
	*elasticsearch.Client
}

func NewClientV9(client *elasticsearch.Client) ESClient {
	return &clientV9{
		Client: client,
	}
}

func (c *clientV9) Search(ctx context.Context, req *SearchRequest) *Response {
	search := c.Client.Search
	opts := []func(*esapiv9.SearchRequest){
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

	data, err := ResponseDecodeV9(res)
	if err != nil {
		return &Response{StatusCode: res.StatusCode, Err: err}
	}

	return &Response{StatusCode: res.StatusCode, Data: data}
}

func (c *clientV9) Get(ctx context.Context, req *GetRequest) *Response {
	get := c.Client.Get
	res, err := get(req.Index, req.DocumentID, get.WithContext(ctx))
	if err != nil {
		return &Response{Err: err}
	}

	data, err := ResponseDecodeV9(res)
	if err != nil {
		return &Response{StatusCode: res.StatusCode, Err: err}
	}

	return &Response{StatusCode: res.StatusCode, Data: data}
}

func (c *clientV9) OpenPointInTime(ctx context.Context, req *OpenPointInTimeRequest) *Response {
	keepAlive := strings.TrimSpace(req.KeepAlive)
	if keepAlive == "" {
		keepAlive = "1m"
	}

	openPointInTime := c.Client.OpenPointInTime
	res, err := openPointInTime(
		[]string{req.Index},
		keepAlive,
		openPointInTime.WithContext(ctx),
	)
	if err != nil {
		return &Response{Err: err}
	}

	data, err := ResponseDecodeV9(res)
	if err != nil {
		return &Response{StatusCode: res.StatusCode, Err: err}
	}

	return &Response{StatusCode: res.StatusCode, Data: data}
}

func (c *clientV9) ClosePointInTime(ctx context.Context, req *ClosePointInTimeRequest) *Response {
	if req == nil || req.Body == nil {
		return &Response{Err: errors.New("close PIT body is required")}
	}
	closePointInTime := c.Client.ClosePointInTime
	res, err := closePointInTime(
		req.Body,
		closePointInTime.WithContext(ctx),
	)
	if err != nil {
		return &Response{Err: err}
	}

	data, err := ResponseDecodeV9(res)
	if err != nil {
		return &Response{StatusCode: res.StatusCode, Err: err}
	}

	return &Response{StatusCode: res.StatusCode, Data: data}
}

func (c *clientV9) CreateIndex(ctx context.Context, req *CreateIndexRequest) *Response {
	createReq := esapiv9.IndicesCreateRequest{
		Index: req.Index,
		Body:  req.Body,
	}
	res, err := createReq.Do(ctx, c.Client)
	if err != nil {
		return &Response{Err: err}
	}

	data, err := ResponseDecodeV9(res)
	if err != nil {
		return &Response{StatusCode: res.StatusCode, Err: err}
	}

	return &Response{StatusCode: res.StatusCode, Data: data}
}

func (c *clientV9) DeleteIndex(ctx context.Context, req *DeleteIndexRequest) *Response {
	deleteReq := esapiv9.IndicesDeleteRequest{
		Index: getIndex(req.Index),
	}
	res, err := deleteReq.Do(ctx, c.Client)
	if err != nil {
		return &Response{Err: err}
	}

	data, err := ResponseDecodeV9(res)
	if err != nil {
		return &Response{StatusCode: res.StatusCode, Err: err}
	}

	return &Response{StatusCode: res.StatusCode, Data: data}
}

func (c *clientV9) Create(ctx context.Context, req *CreateRequest) *Response {
	createReq := esapiv9.CreateRequest{
		Index:      req.Index,
		DocumentID: req.DocumentID,
		Body:       req.Body,
	}

	res, err := createReq.Do(ctx, c.Client)
	if err != nil {
		return &Response{Err: err}
	}

	data, err := ResponseDecodeV9(res)
	if err != nil {
		return &Response{StatusCode: res.StatusCode, Err: err}
	}

	return &Response{StatusCode: res.StatusCode, Data: data}
}
