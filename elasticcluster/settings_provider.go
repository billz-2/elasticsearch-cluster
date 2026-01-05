package elasticcluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"
)

type GetESSettingsRes struct {
	ClusterID   int    `json:"cluster_id"`
	Version     int    `json:"version"`
	ClusterName string `json:"cluster_name"`
	IndexName   string `json:"index_name"`
}

type SettingsProvider interface {
	GetSettings(ctx context.Context, companyID, indexType string) (GetESSettingsRes, error)
}

type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type settingsProvider struct {
	elasticSyncServiceURL string
	cache                 *redis.Client
	httpClient            HTTPClient
}

func NewSettingsProvider(
	elasticSyncServiceURL string,
	cache *redis.Client,
	httpClient HTTPClient,
) SettingsProvider {
	return &settingsProvider{
		elasticSyncServiceURL: elasticSyncServiceURL,
		cache:                 cache,
		httpClient:            httpClient,
	}
}

type RefreshCompanyESCacheReq struct {
	CompanyID string `json:"company_id"`
	Type      string `json:"type"` // product_tree, product_flat, import, …)
}

func (sp *settingsProvider) GetSettings(
	ctx context.Context,
	companyID, indexType string,
) (GetESSettingsRes, error) {
	redisKey := fmt.Sprintf("es_settings_%s_%s", companyID, indexType)

	if sp.cache != nil {
		var settingsBytes []byte
		if err := sp.cache.Get(ctx, redisKey).Scan(&settingsBytes); err == nil {
			var settings GetESSettingsRes
			if err := json.Unmarshal(settingsBytes, &settings); err == nil {
				return settings, nil
			}
		}
	}

	reqBody := RefreshCompanyESCacheReq{
		CompanyID: companyID,
		Type:      indexType,
	}
	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return GetESSettingsRes{}, fmt.Errorf("failed to marshal request body: %w", err)
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		sp.elasticSyncServiceURL+"/v1/company/refresh-es-info-cache",
		bytes.NewReader(jsonBody),
	)
	if err != nil {
		return GetESSettingsRes{}, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := sp.httpClient.Do(req)
	if err != nil {
		return GetESSettingsRes{}, fmt.Errorf("failed to execute request: %w", err)
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return GetESSettingsRes{}, fmt.Errorf("bad status: %s", resp.Status)
	}

	var settings GetESSettingsRes
	if err := json.NewDecoder(resp.Body).Decode(&settings); err != nil {
		return GetESSettingsRes{}, fmt.Errorf("failed to decode response: %w", err)
	}

	if sp.cache != nil {
		if cacheBytes, err := json.Marshal(settings); err == nil {
			go func() {
				ctxCache, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
				defer cancel()
				_ = sp.cache.Set(
					ctxCache,
					redisKey,
					cacheBytes,
					time.Hour*24,
				).Err()
			}()
		}
	}

	return settings, nil
}
