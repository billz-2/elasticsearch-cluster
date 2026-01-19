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
	registry       *Registry
	redis          *redis.Client
	syncURL        string
	cacheTTL       time.Duration
	httpClient     *http.Client
	defaultClient  *Client            // cached default client
	clients        map[string]*Client // cached clients by cluster name
	log            Logger             // logger for debugging
	indexPrefixMap map[string]string  // mapping: indexType -> index name prefix
}

// ResolverConfig configures the resolver.
type ResolverConfig struct {
	Registry       *Registry         // Registry with pre-created clients
	Redis          *redis.Client     // Redis client for caching
	SyncURL        string            // Sync service URL (e.g., "http://sync-service:8080")
	CacheTTL       time.Duration     // Cache TTL (default: 24h)
	HTTPClient     *http.Client      // HTTP client for sync calls (optional)
	Logger         Logger            // Logger for debugging (optional)
	IndexPrefixMap map[string]string // Optional custom mapping: indexType -> index name prefix
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

	// Set default index prefix mapping if not provided
	indexPrefixMap := cfg.IndexPrefixMap
	if indexPrefixMap == nil {
		indexPrefixMap = map[string]string{
			"product_tree": "products_",
		}
	}

	// Pre-create all clients from registry
	clusterNames := cfg.Registry.ListClusters()
	clients := make(map[string]*Client, len(clusterNames))

	for _, clusterName := range clusterNames {
		entry, err := cfg.Registry.GetEntry(clusterName)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get entry for cluster %q", clusterName)
		}

		baseURL, err := parseBaseURL(entry.BaseURL)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse base URL for cluster %q", clusterName)
		}

		clients[clusterName] = &Client{
			es:      entry.ES,
			baseURL: baseURL,
			log:     safeLogger(cfg.Logger),
		}
	}

	// Get default client
	defaultEntry, err := cfg.Registry.GetEntry("")
	if err != nil {
		return nil, errors.Wrap(err, "failed to get default cluster entry")
	}
	defaultClient := clients[defaultEntry.Name]

	return &Resolver{
		registry:       cfg.Registry,
		redis:          cfg.Redis,
		syncURL:        cfg.SyncURL,
		cacheTTL:       cfg.CacheTTL,
		httpClient:     cfg.HTTPClient,
		defaultClient:  defaultClient,
		clients:        clients,
		log:            safeLogger(cfg.Logger),
		indexPrefixMap: indexPrefixMap,
	}, nil
}

// getIndexPrefix returns the index name prefix for indexType.
// If no mapping found, returns the indexType itself with underscore.
func (r *Resolver) getIndexPrefix(indexType string) string {
	if prefix, ok := r.indexPrefixMap[indexType]; ok {
		return prefix
	}
	return indexType + "_"
}

// Resolve resolves cluster and index for company and index type.
// Returns typed client and index name.
// If sync service returns empty response (index not migrated yet),
// returns default cluster client and index name in format: <indexType>_<companyID>
func (r *Resolver) Resolve(ctx context.Context, companyID, indexType string) (*Client, string, error) {
	if companyID == "" {
		return nil, "", errors.New("company ID is required")
	}
	if indexType == "" {
		return nil, "", errors.New("index type is required")
	}

	r.log.DebugWithCtx(ctx, "elasticsearch resolver resolve", map[string]interface{}{
		"company_id": companyID,
		"index_type": indexType,
	})

	// 1. Try Redis cache
	info, err := r.getFromCache(ctx, companyID, indexType)
	if err == nil && info != nil && info.ClusterName != "" {
		r.log.DebugWithCtx(ctx, "elasticsearch resolver cache hit", map[string]interface{}{
			"cluster_name": info.ClusterName,
			"index_name":   info.IndexName,
		})
		client, err := r.getClient(info.ClusterName)
		return client, info.IndexName, err
	}

	r.log.DebugWithCtx(ctx, "elasticsearch resolver cache miss", nil)

	// 2. Fetch from sync service
	info, err = r.fetchFromSync(ctx, companyID, indexType)
	if err != nil {
		return nil, "", errors.Wrap(err, "failed to fetch from sync service")
	}

	// 3. If sync returned empty info, index not migrated yet - use default cluster
	// DON'T cache this - we want to check sync service again after migration
	if info == nil || info.ClusterName == "" {
		prefix := r.getIndexPrefix(indexType)
		indexName := fmt.Sprintf("%s%s", prefix, companyID)
		r.log.DebugWithCtx(ctx, "elasticsearch resolver using default cluster (not migrated)", map[string]interface{}{
			"index_name": indexName,
		})
		return r.defaultClient, indexName, nil
	}

	r.log.DebugWithCtx(ctx, "elasticsearch resolver resolved from sync", map[string]interface{}{
		"cluster_name": info.ClusterName,
		"index_name":   info.IndexName,
	})

	// 4. Save to cache asynchronously with timeout (only cache migrated indices)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = r.saveToCache(ctx, companyID, indexType, info)
	}()

	// 5. Get cached client
	client, err := r.getClient(info.ClusterName)
	return client, info.IndexName, err
}

// ResolveRaw resolves cluster info without creating client.
// Useful when you need just the cluster name and index.
// If sync service returns empty response (index not migrated yet),
// returns default cluster info with index name in format: <indexType>_<companyID>
func (r *Resolver) ResolveRaw(ctx context.Context, companyID, indexType string) (*ClusterInfo, error) {
	if companyID == "" {
		return nil, errors.New("company ID is required")
	}
	if indexType == "" {
		return nil, errors.New("index type is required")
	}

	// Try cache first
	info, err := r.getFromCache(ctx, companyID, indexType)
	if err == nil && info != nil && info.ClusterName != "" {
		return info, nil
	}

	// Fetch from sync
	info, err = r.fetchFromSync(ctx, companyID, indexType)
	if err != nil {
		return nil, err
	}

	// If sync returned empty info, index not migrated yet - use default cluster
	// DON'T cache this - we want to check sync service again after migration
	if info == nil || info.ClusterName == "" {
		defaultEntry, err := r.registry.GetEntry("")
		if err != nil {
			return nil, errors.Wrap(err, "failed to get default cluster entry")
		}
		prefix := r.getIndexPrefix(indexType)
		return &ClusterInfo{
			ClusterName: defaultEntry.Name,
			ClusterID:   0,
			IndexName:   fmt.Sprintf("%s%s", prefix, companyID),
		}, nil
	}

	// Cache asynchronously with timeout (only cache migrated indices)
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
// Returns nil info if sync returns empty response or error (index not migrated).
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

	// If sync service returns 400/404, it means index not migrated yet
	if resp.StatusCode == http.StatusBadRequest || resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("sync service returned status %d: %s", resp.StatusCode, string(body))
	}

	var info ClusterInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return nil, errors.Wrap(err, "failed to decode sync response")
	}

	// If cluster name is empty, sync returned empty response (not migrated yet)
	if info.ClusterName == "" {
		return nil, nil
	}

	return &info, nil
}

// getClient returns cached client from map by cluster name.
func (r *Resolver) getClient(clusterName string) (*Client, error) {
	client, ok := r.clients[clusterName]
	if !ok {
		return nil, errors.Errorf("cluster %q not found in clients map", clusterName)
	}
	return client, nil
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
