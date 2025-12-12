package elasticcluster

import (
	"context"

	"github.com/billz-2/elasticsearch-cluster/settingsprovider"
)

const (
	ESIndexTypeProductTree = "product_tree"
	ESIndexTypeOrder       = "order"
)

type BaseRepository interface {
	Search(ctx context.Context, req *SearchRequest) *Response
	OpenPointInTime(ctx context.Context, req *OpenPointInTimeRequest) *Response
	ClosePointInTime(ctx context.Context, req *ClosePointInTimeRequest) *Response

	CreateIndex(ctx context.Context, req *CreateIndexRequest) *Response
	DeleteIndex(ctx context.Context, req *DeleteIndexRequest) *Response

	Create(ctx context.Context, req *CreateRequest) *Response
}

type baseRepository struct {
	settingsProvider settingsprovider.SettingsProvider
	resolver         *Resolver
}

func NewBaseRepository(settingsProvider settingsprovider.SettingsProvider, resolver *Resolver) BaseRepository {
	return &baseRepository{
		settingsProvider: settingsProvider,
		resolver:         resolver,
	}
}

func (br *baseRepository) Search(ctx context.Context, req *SearchRequest) *Response {
	return br.withClient(ctx, req.CompanyID, req.IndexType,
		func(es ESClient, indexName string) *Response {
			if req.Index == "" {
				req.Index = indexName
			}
			return es.Search(ctx, req)
		})
}

func (br *baseRepository) OpenPointInTime(ctx context.Context, req *OpenPointInTimeRequest) *Response {
	return br.withClient(ctx, req.CompanyID, req.IndexType,
		func(es ESClient, indexName string) *Response {
			if req.Index == "" {
				req.Index = indexName
			}
			return es.OpenPointInTime(ctx, req)
		})
}

func (br *baseRepository) ClosePointInTime(ctx context.Context, req *ClosePointInTimeRequest) *Response {
	return br.withClient(ctx, req.CompanyID, req.IndexType,
		func(es ESClient, _ string) *Response {
			return es.ClosePointInTime(ctx, req)
		})
}

func (br *baseRepository) CreateIndex(ctx context.Context, req *CreateIndexRequest) *Response {
	return br.withClient(ctx, req.CompanyID, req.IndexType,
		func(es ESClient, indexName string) *Response {
			if req.Index == "" {
				req.Index = indexName
			}
			return es.CreateIndex(ctx, req)
		})
}

func (br *baseRepository) DeleteIndex(ctx context.Context, req *DeleteIndexRequest) *Response {
	return br.withClient(ctx, req.CompanyID, req.IndexType,
		func(es ESClient, indexName string) *Response {
			if req.Index == "" {
				req.Index = indexName
			}
			return es.DeleteIndex(ctx, req)
		})
}

func (br *baseRepository) Create(ctx context.Context, req *CreateRequest) *Response {
	return br.withClient(ctx, req.CompanyID, req.IndexType,
		func(es ESClient, indexName string) *Response {
			if req.Index == "" {
				req.Index = indexName
			}
			return es.Create(ctx, req)
		})
}

func (br *baseRepository) withClient(
	ctx context.Context,
	companyID, indexType string,
	fn func(ESClient, string) *Response,
) *Response {
	indexNameStr, es := br.resolve(ctx, companyID, indexType)

	return fn(es, indexNameStr)
}

func (br *baseRepository) resolve(
	ctx context.Context,
	companyID, indexType string,
) (string, ESClient) {
	if br.settingsProvider != nil && br.resolver != nil && companyID != "" && indexType != "" {
		settings, err := br.settingsProvider.GetSettings(ctx, companyID, indexType)
		if err != nil {
			return "", errorClient{err: err}
		}
		client := br.resolver.Get(settings.ClusterName, settings.Version)
		if client == nil {
			return "", errorClient{err: ErrNoClient}
		}

		indexName := settings.IndexName
		if indexName == "" { // if settings is not set, use default index
			switch indexType {
			case ESIndexTypeProductTree:
				indexName = getDefaultProductsIndex(companyID)
			case ESIndexTypeOrder:
				indexName = getDefaultOrdersIndex()
			default:
				return "", errorClient{err: ErrInvalidIndexType}
			}
		}

		return indexName, client
	}

	return "", errorClient{err: ErrNoClient}
}

// getDefaultProductsIndex returns the default index("products_{companyID}") for products
func getDefaultProductsIndex(companyID string) string {
	return "products_" + companyID
}

// getDefaultOrdersIndex returns the default index("orders_all") for orders
func getDefaultOrdersIndex() string {
	return "orders_all"
}
