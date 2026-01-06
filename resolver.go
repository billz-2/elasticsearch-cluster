package esclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
)

// ClusterInfo represents routing information from sync service.
type ClusterInfo struct {
	ClusterName string `json:"cluster_name"`
	ClusterID   int    `json:"cluster_id"`
	IndexName   string `json:"index_name"`
}

// Resolver resolves cluster and index for company using Redis cache and sync service.
type Resolver struct {
	registry   *Registry
	redis      *redis.Client
	syncURL    string
	cacheTTL   time.Duration
	httpClient *http.Client
}

// ResolverConfig configures the resolver.
type ResolverConfig struct {
	Registry   *Registry     // Registry with pre-created clients
	Redis      *redis.Client // Redis client for caching
	SyncURL    string        // Sync service URL (e.g., "http://sync-service:8080")
	CacheTTL   time.Duration // Cache TTL (default: 24h)
	HTTPClient *http.Client  // HTTP client for sync calls (optional)
}

// NewResolver creates a new resolver with Redis caching.
func NewResolver(cfg ResolverConfig) (*Resolver, error) {
	if cfg.Registry == nil {
		return nil, errors.New("registry is required")
	}
	if cfg.Redis == nil {
		return nil, errors.New("redis client is required")
	}
	if cfg.SyncURL == "" {
		return nil, errors.New("sync service URL is required")
	}

	if cfg.CacheTTL == 0 {
		cfg.CacheTTL = 24 * time.Hour
	}

	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{
			Timeout: 5 * time.Second,
		}
	}

	return &Resolver{
		registry:   cfg.Registry,
		redis:      cfg.Redis,
		syncURL:    cfg.SyncURL,
		cacheTTL:   cfg.CacheTTL,
		httpClient: cfg.HTTPClient,
	}, nil
}

// Resolve resolves cluster and index for company and index type.
// Returns typed client and index name.
func (r *Resolver) Resolve(ctx context.Context, companyID, indexType string) (*Client, string, error) {
	if companyID == "" {
		return nil, "", errors.New("company ID is required")
	}
	if indexType == "" {
		return nil, "", errors.New("index type is required")
	}

	// 1. Try Redis cache
	info, err := r.getFromCache(ctx, companyID, indexType)
	if err == nil && info != nil {
		client, err := r.createClient(info)
		return client, info.IndexName, err
	}

	// 2. Fetch from sync service
	info, err = r.fetchFromSync(ctx, companyID, indexType)
	if err != nil {
		return nil, "", errors.Wrap(err, "failed to fetch from sync service")
	}

	// 3. Save to cache asynchronously with timeout
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = r.saveToCache(ctx, companyID, indexType, info)
	}()

	// 4. Create client
	client, err := r.createClient(info)
	return client, info.IndexName, err
}

// ResolveRaw resolves cluster info without creating client.
// Useful when you need just the cluster name and index.
func (r *Resolver) ResolveRaw(ctx context.Context, companyID, indexType string) (*ClusterInfo, error) {
	if companyID == "" {
		return nil, errors.New("company ID is required")
	}
	if indexType == "" {
		return nil, errors.New("index type is required")
	}

	// Try cache first
	info, err := r.getFromCache(ctx, companyID, indexType)
	if err == nil && info != nil {
		return info, nil
	}

	// Fetch from sync
	info, err = r.fetchFromSync(ctx, companyID, indexType)
	if err != nil {
		return nil, err
	}

	// Cache asynchronously with timeout
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = r.saveToCache(ctx, companyID, indexType, info)
	}()

	return info, nil
}

// getFromCache retrieves cluster info from Redis.
func (r *Resolver) getFromCache(ctx context.Context, companyID, indexType string) (*ClusterInfo, error) {
	key := fmt.Sprintf("es_settings_%s_%s", companyID, indexType)

	val, err := r.redis.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, errors.New("cache miss")
		}
		return nil, errors.Wrap(err, "redis get failed")
	}

	var info ClusterInfo
	if err := json.Unmarshal([]byte(val), &info); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal cached info")
	}

	return &info, nil
}

// saveToCache saves cluster info to Redis.
func (r *Resolver) saveToCache(ctx context.Context, companyID, indexType string, info *ClusterInfo) error {
	key := fmt.Sprintf("es_settings_%s_%s", companyID, indexType)

	data, err := json.Marshal(info)
	if err != nil {
		return errors.Wrap(err, "failed to marshal info")
	}

	if err := r.redis.Set(ctx, key, data, r.cacheTTL).Err(); err != nil {
		return errors.Wrap(err, "redis set failed")
	}

	return nil
}

// fetchFromSync calls sync service to get cluster info.
func (r *Resolver) fetchFromSync(ctx context.Context, companyID, indexType string) (*ClusterInfo, error) {
	url := fmt.Sprintf("%s/v1/company/refresh-es-info-cache", r.syncURL)

	reqBody := map[string]string{
		"company_id": companyID,
		"type":       indexType,
	}

	bodyReader, err := jsonBody(reqBody)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create request body")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bodyReader)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create HTTP request")
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "HTTP request to sync service failed")
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("sync service returned status %d: %s", resp.StatusCode, string(body))
	}

	var info ClusterInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return nil, errors.Wrap(err, "failed to decode sync response")
	}

	return &info, nil
}

// createClient creates typed client from cluster info.
func (r *Resolver) createClient(info *ClusterInfo) (*Client, error) {
	entry, err := r.registry.GetEntry(info.ClusterName)
	if err != nil {
		return nil, errors.Wrapf(err, "cluster %q not found in registry", info.ClusterName)
	}

	baseURL, err := parseBaseURL(entry.BaseURL)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse base URL")
	}

	return &Client{
		es:      entry.ES,
		baseURL: baseURL,
	}, nil
}

// InvalidateCache removes cached cluster info for company and index type.
func (r *Resolver) InvalidateCache(ctx context.Context, companyID, indexType string) error {
	key := fmt.Sprintf("es_settings_%s_%s", companyID, indexType)
	return r.redis.Del(ctx, key).Err()
}

// InvalidateCompanyCache removes all cached cluster info for a company.
func (r *Resolver) InvalidateCompanyCache(ctx context.Context, companyID string) error {
	pattern := fmt.Sprintf("es_settings_%s_*", companyID)

	iter := r.redis.Scan(ctx, 0, pattern, 0).Iterator()
	for iter.Next(ctx) {
		if err := r.redis.Del(ctx, iter.Val()).Err(); err != nil {
			return errors.Wrapf(err, "failed to delete key %s", iter.Val())
		}
	}

	return iter.Err()
}
