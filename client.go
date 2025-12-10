package elasticcluster

import "context"

const (
	ESVersion8 = 8
	ESVersion9 = 9
)

type ESClient interface {
	Search(ctx context.Context, req *SearchRequest) *Response
	OpenPointInTime(ctx context.Context, req *OpenPointInTimeRequest) *Response
	ClosePointInTime(ctx context.Context, req *ClosePointInTimeRequest) *Response
	CreateIndex(ctx context.Context, req *CreateIndexRequest) *Response
	DeleteIndex(ctx context.Context, req *DeleteIndexRequest) *Response
	Create(ctx context.Context, req *CreateRequest) *Response
}

type errorClient struct {
	err error
}

func (c errorClient) Search(_ context.Context, _ *SearchRequest) *Response {
	return &Response{Err: c.err}
}

func (c errorClient) OpenPointInTime(_ context.Context, _ *OpenPointInTimeRequest) *Response {
	return &Response{Err: c.err}
}

func (c errorClient) ClosePointInTime(_ context.Context, _ *ClosePointInTimeRequest) *Response {
	return &Response{Err: c.err}
}

func (c errorClient) CreateIndex(_ context.Context, _ *CreateIndexRequest) *Response {
	return &Response{Err: c.err}
}

func (c errorClient) DeleteIndex(_ context.Context, _ *DeleteIndexRequest) *Response {
	return &Response{Err: c.err}
}

func (c errorClient) Create(_ context.Context, _ *CreateRequest) *Response {
	return &Response{Err: c.err}
}

// defaultErrClient is returned when no concrete ES client can be resolved.
type defaultErrClient struct{}

func (defaultErrClient) Search(_ context.Context, _ *SearchRequest) *Response {
	return &Response{Err: ErrNoClient}
}

func (defaultErrClient) OpenPointInTime(_ context.Context, _ *OpenPointInTimeRequest) *Response {
	return &Response{Err: ErrNoClient}
}

func (defaultErrClient) ClosePointInTime(_ context.Context, _ *ClosePointInTimeRequest) *Response {
	return &Response{Err: ErrNoClient}
}

func (defaultErrClient) CreateIndex(_ context.Context, _ *CreateIndexRequest) *Response {
	return &Response{Err: ErrNoClient}
}

func (defaultErrClient) DeleteIndex(_ context.Context, _ *DeleteIndexRequest) *Response {
	return &Response{Err: ErrNoClient}
}

func (defaultErrClient) Create(_ context.Context, _ *CreateRequest) *Response {
	return &Response{Err: ErrNoClient}
}
