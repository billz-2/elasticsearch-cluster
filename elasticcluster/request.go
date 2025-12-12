package elasticcluster

import (
	"bytes"
	"io"

	"github.com/elastic/go-elasticsearch/v8/esapi"
	esapiv9 "github.com/elastic/go-elasticsearch/v9/esapi"
)

type ElasticClusterCreds struct {
	User      string
	Password  string
	Addresses []string
	Version   int
}

type SearchRequest struct {
	CompanyID          string
	IndexType          string
	Body               *bytes.Buffer
	Index              string
	WithTrackTotalHits bool
	WithPretty         bool
	From               int
	Size               int
}

func (sr *SearchRequest) GetRequest() esapi.SearchRequest {
	return esapi.SearchRequest{
		Index:          getIndex(sr.Index),
		Body:           sr.Body,
		Pretty:         sr.WithPretty,
		TrackTotalHits: sr.WithTrackTotalHits,
		From:           &sr.From,
		Size:           &sr.Size,
	}
}

func (sr *SearchRequest) GetRequestV9() esapiv9.SearchRequest {
	return esapiv9.SearchRequest{
		Index:          getIndex(sr.Index),
		Body:           sr.Body,
		Pretty:         sr.WithPretty,
		TrackTotalHits: sr.WithTrackTotalHits,
		From:           &sr.From,
		Size:           &sr.Size,
	}
}

func getIndex(idx string) []string {
	if idx == "" {
		return nil
	}
	return []string{idx}
}

type OpenPointInTimeRequest struct {
	CompanyID string
	IndexType string
	Index     string
	KeepAlive string
}

func (opitr *OpenPointInTimeRequest) GetKeepAlive() string {
	if opitr.KeepAlive == "" {
		opitr.KeepAlive = "1m"
	}

	return opitr.KeepAlive
}

type ClosePointInTime struct {
	CompanyID string
	IndexType string
	ID        string
}

type ClosePointInTimeRequest struct {
	CompanyID string
	IndexType string
	Body      *bytes.Buffer
}

// https://www.elastic.co/guide/en/elasticsearch/reference/master/indices-create-index.html
type CreateIndexRequest struct {
	CompanyID string
	IndexType string
	Index     string
	Body      io.Reader
}

func (cir *CreateIndexRequest) GetRequest() esapi.IndicesCreateRequest {
	return esapi.IndicesCreateRequest{
		Index: cir.Index,
		Body:  cir.Body,
	}
}

type DeleteIndexRequest struct {
	CompanyID string
	IndexType string
	Index     string
}

func (dir *DeleteIndexRequest) GetRequest() esapi.IndicesDeleteRequest {
	return esapi.IndicesDeleteRequest{
		Index: []string{dir.Index},
	}
}

type CreateRequest struct {
	CompanyID  string
	IndexType  string
	Index      string
	DocumentID string
	Body       io.Reader
}

func (cir *CreateRequest) GetRequest() esapi.CreateRequest {
	return esapi.CreateRequest{
		Index:      cir.Index,
		Body:       cir.Body,
		DocumentID: cir.DocumentID,
	}
}
