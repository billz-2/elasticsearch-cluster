package e2e

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	esclient "github.com/billz-2/elasticsearch-cluster"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolverCaching(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	t.Run("cache_cluster_info", func(t *testing.T) {
		companyID := "company_123"
		indexType := "orders"
		key := fmt.Sprintf("es_settings_%s_%s", companyID, indexType)

		// Prepare cluster info
		info := esclient.ClusterInfo{
			ClusterName: "test-cluster",
			ClusterID:   1,
			IndexName:   "orders_test",
		}

		// Cache cluster info
		data, err := json.Marshal(info)
		require.NoError(t, err)

		err = redisClient.Set(ctx, key, data, 10*time.Second).Err()
		require.NoError(t, err)

		// Retrieve from cache
		val, err := redisClient.Get(ctx, key).Result()
		require.NoError(t, err)

		var retrieved esclient.ClusterInfo
		err = json.Unmarshal([]byte(val), &retrieved)
		require.NoError(t, err)

		assert.Equal(t, info.ClusterName, retrieved.ClusterName)
		assert.Equal(t, info.ClusterID, retrieved.ClusterID)
		assert.Equal(t, info.IndexName, retrieved.IndexName)
	})

	t.Run("cache_expiration", func(t *testing.T) {
		key := "test_cache_expiration"
		value := "test_value"

		// Set with 1 second TTL
		err := redisClient.Set(ctx, key, value, 1*time.Second).Err()
		require.NoError(t, err)

		// Should exist immediately
		val, err := redisClient.Get(ctx, key).Result()
		require.NoError(t, err)
		assert.Equal(t, value, val)

		// Wait for expiration
		time.Sleep(1100 * time.Millisecond)

		// Should not exist
		_, err = redisClient.Get(ctx, key).Result()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "redis: nil")
	})

	t.Run("invalidate_cache", func(t *testing.T) {
		companyID := "company_456"
		indexType := "products"
		key := fmt.Sprintf("es_settings_%s_%s", companyID, indexType)

		// Set cache
		err := redisClient.Set(ctx, key, "test_data", 10*time.Minute).Err()
		require.NoError(t, err)

		// Verify exists
		exists, err := redisClient.Exists(ctx, key).Result()
		require.NoError(t, err)
		assert.Equal(t, int64(1), exists)

		// Invalidate (delete)
		err = redisClient.Del(ctx, key).Err()
		require.NoError(t, err)

		// Verify deleted
		exists, err = redisClient.Exists(ctx, key).Result()
		require.NoError(t, err)
		assert.Equal(t, int64(0), exists)
	})

	t.Run("invalidate_company_cache_pattern", func(t *testing.T) {
		companyID := "company_789"

		// Create multiple cache entries for company
		keys := []string{
			fmt.Sprintf("es_settings_%s_orders", companyID),
			fmt.Sprintf("es_settings_%s_products", companyID),
			fmt.Sprintf("es_settings_%s_customers", companyID),
		}

		for _, key := range keys {
			err := redisClient.Set(ctx, key, "test", 10*time.Minute).Err()
			require.NoError(t, err)
		}

		// Verify all exist
		for _, key := range keys {
			exists, err := redisClient.Exists(ctx, key).Result()
			require.NoError(t, err)
			assert.Equal(t, int64(1), exists)
		}

		// Delete all keys for company using pattern
		pattern := fmt.Sprintf("es_settings_%s_*", companyID)
		iter := redisClient.Scan(ctx, 0, pattern, 0).Iterator()
		var deleted int
		for iter.Next(ctx) {
			err := redisClient.Del(ctx, iter.Val()).Err()
			require.NoError(t, err)
			deleted++
		}
		require.NoError(t, iter.Err())
		assert.Equal(t, 3, deleted)

		// Verify all deleted
		for _, key := range keys {
			exists, err := redisClient.Exists(ctx, key).Result()
			require.NoError(t, err)
			assert.Equal(t, int64(0), exists)
		}
	})
}

func TestResolverConfiguration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	t.Run("create_resolver_valid_config", func(t *testing.T) {
		resolver, err := esclient.NewResolver(esclient.ResolverConfig{
			Registry:   registry,
			Redis:      redisClient,
			SyncURL:    "http://localhost:8080",
			CacheTTL:   24 * time.Hour,
			HTTPClient: nil, // will use default
		})

		require.NoError(t, err)
		assert.NotNil(t, resolver)
	})

	t.Run("create_resolver_missing_registry", func(t *testing.T) {
		_, err := esclient.NewResolver(esclient.ResolverConfig{
			Registry: nil,
			Redis:    redisClient,
			SyncURL:  "http://localhost:8080",
		})

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "registry is required")
	})

	t.Run("create_resolver_missing_redis", func(t *testing.T) {
		_, err := esclient.NewResolver(esclient.ResolverConfig{
			Registry: registry,
			Redis:    nil,
			SyncURL:  "http://localhost:8080",
		})

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "redis client is required")
	})

	t.Run("create_resolver_missing_sync_url", func(t *testing.T) {
		_, err := esclient.NewResolver(esclient.ResolverConfig{
			Registry: registry,
			Redis:    redisClient,
			SyncURL:  "",
		})

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "sync service URL is required")
	})

	t.Run("create_resolver_default_cache_ttl", func(t *testing.T) {
		resolver, err := esclient.NewResolver(esclient.ResolverConfig{
			Registry: registry,
			Redis:    redisClient,
			SyncURL:  "http://localhost:8080",
			// CacheTTL not specified, should default to 24h
		})

		require.NoError(t, err)
		assert.NotNil(t, resolver)
	})
}

func TestRedisOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	t.Run("ping", func(t *testing.T) {
		err := redisClient.Ping(ctx).Err()
		require.NoError(t, err)
	})

	t.Run("set_and_get", func(t *testing.T) {
		key := "test_key"
		value := "test_value"

		err := redisClient.Set(ctx, key, value, 0).Err()
		require.NoError(t, err)

		val, err := redisClient.Get(ctx, key).Result()
		require.NoError(t, err)
		assert.Equal(t, value, val)

		// Cleanup
		redisClient.Del(ctx, key)
	})

	t.Run("set_with_ttl", func(t *testing.T) {
		key := "test_ttl"
		value := "test_value"

		err := redisClient.Set(ctx, key, value, 1*time.Hour).Err()
		require.NoError(t, err)

		ttl, err := redisClient.TTL(ctx, key).Result()
		require.NoError(t, err)
		assert.Greater(t, ttl, 59*time.Minute) // Should be close to 1 hour
		assert.LessOrEqual(t, ttl, 1*time.Hour)

		// Cleanup
		redisClient.Del(ctx, key)
	})

	t.Run("delete_nonexistent_key", func(t *testing.T) {
		err := redisClient.Del(ctx, "nonexistent_key").Err()
		require.NoError(t, err) // Delete of nonexistent key doesn't error
	})
}

func TestResolverFallbackToDefault(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	t.Run("index_not_migrated_400_response", func(t *testing.T) {
		// Mock sync service that returns 400 (index not migrated)
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"error": "no settings found"}`))
		}))
		defer mockServer.Close()

		resolver, err := esclient.NewResolver(esclient.ResolverConfig{
			Registry: registry,
			Redis:    redisClient,
			SyncURL:  mockServer.URL,
			CacheTTL: 1 * time.Minute,
		})
		require.NoError(t, err)

		companyID := "test-company-uuid-123"
		indexType := "products"

		client, indexName, err := resolver.Resolve(ctx, companyID, indexType)
		require.NoError(t, err)
		require.NotNil(t, client)

		// Should return default client with index name: <indexType>_<companyID>
		expectedIndexName := fmt.Sprintf("%s_%s", indexType, companyID)
		assert.Equal(t, expectedIndexName, indexName)
	})

	t.Run("index_not_migrated_404_response", func(t *testing.T) {
		// Mock sync service that returns 404 (index not migrated)
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer mockServer.Close()

		resolver, err := esclient.NewResolver(esclient.ResolverConfig{
			Registry: registry,
			Redis:    redisClient,
			SyncURL:  mockServer.URL,
			CacheTTL: 1 * time.Minute,
		})
		require.NoError(t, err)

		companyID := "test-company-uuid-456"
		indexType := "orders"

		client, indexName, err := resolver.Resolve(ctx, companyID, indexType)
		require.NoError(t, err)
		require.NotNil(t, client)

		// Should return default client with index name: <indexType>_<companyID>
		expectedIndexName := fmt.Sprintf("%s_%s", indexType, companyID)
		assert.Equal(t, expectedIndexName, indexName)
	})

	t.Run("index_migrated_success_response", func(t *testing.T) {
		// Mock sync service that returns success (index migrated)
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			response := esclient.ClusterInfo{
				ClusterName: "tier-gold",
				ClusterID:   2,
				IndexName:   "migrated_index_name",
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(response)
		}))
		defer mockServer.Close()

		resolver, err := esclient.NewResolver(esclient.ResolverConfig{
			Registry: registry,
			Redis:    redisClient,
			SyncURL:  mockServer.URL,
			CacheTTL: 1 * time.Minute,
		})
		require.NoError(t, err)

		companyID := "test-company-uuid-789"
		indexType := "products"

		client, indexName, err := resolver.Resolve(ctx, companyID, indexType)
		require.NoError(t, err)
		require.NotNil(t, client)

		// Should return migrated index name from sync service
		assert.Equal(t, "migrated_index_name", indexName)
	})

	t.Run("index_not_migrated_empty_cluster_name", func(t *testing.T) {
		// Mock sync service that returns empty cluster name
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			response := esclient.ClusterInfo{
				ClusterName: "",
				ClusterID:   0,
				IndexName:   "",
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(response)
		}))
		defer mockServer.Close()

		resolver, err := esclient.NewResolver(esclient.ResolverConfig{
			Registry: registry,
			Redis:    redisClient,
			SyncURL:  mockServer.URL,
			CacheTTL: 1 * time.Minute,
		})
		require.NoError(t, err)

		companyID := "test-company-uuid-999"
		indexType := "orders"

		client, indexName, err := resolver.Resolve(ctx, companyID, indexType)
		require.NoError(t, err)
		require.NotNil(t, client)

		// Should return default client with index name: <indexType>_<companyID>
		expectedIndexName := fmt.Sprintf("%s_%s", indexType, companyID)
		assert.Equal(t, expectedIndexName, indexName)
	})

	t.Run("resolve_raw_index_not_migrated", func(t *testing.T) {
		// Mock sync service that returns 400 (index not migrated)
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"error": "no settings found"}`))
		}))
		defer mockServer.Close()

		resolver, err := esclient.NewResolver(esclient.ResolverConfig{
			Registry: registry,
			Redis:    redisClient,
			SyncURL:  mockServer.URL,
			CacheTTL: 1 * time.Minute,
		})
		require.NoError(t, err)

		companyID := "test-company-uuid-raw"
		indexType := "products"

		info, err := resolver.ResolveRaw(ctx, companyID, indexType)
		require.NoError(t, err)
		require.NotNil(t, info)

		// Should return default cluster info with index name: <indexType>_<companyID>
		expectedIndexName := fmt.Sprintf("%s_%s", indexType, companyID)
		assert.Equal(t, expectedIndexName, info.IndexName)
		assert.NotEmpty(t, info.ClusterName) // Should have default cluster name
	})
}
