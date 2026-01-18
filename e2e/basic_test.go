package e2e

import (
	"bytes"
	"encoding/json"
	"testing"

	esclient "github.com/billz-2/elasticsearch-cluster"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBasicOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	deps := newTestDeps(t, "tier-gold")

	t.Run("create_index", func(t *testing.T) {
		indexName := "test_create_index"

		// Check index doesn't exist
		exists, err := deps.Client.IndexExists(ctx, indexName)
		require.NoError(t, err)
		assert.False(t, exists)

		// Create index
		mapping := map[string]any{
			"mappings": map[string]any{
				"properties": map[string]any{
					"title": map[string]any{"type": "text"},
					"price": map[string]any{"type": "float"},
				},
			},
		}
		mappingBytes, _ := json.Marshal(mapping)

		err = deps.Client.CreateIndex(ctx, &esclient.CreateIndexRequest{
			Index: indexName,
			Body:  bytes.NewReader(mappingBytes),
		})
		require.NoError(t, err)

		// Verify index exists
		exists, err = deps.Client.IndexExists(ctx, indexName)
		require.NoError(t, err)
		assert.True(t, exists)

		// Cleanup
		err = deps.Client.DeleteIndex(ctx, indexName)
		require.NoError(t, err)
	})

	t.Run("search_empty_index", func(t *testing.T) {
		indexName := "test_search_empty"
		deps.createTestIndex(t, indexName)

		query := map[string]any{
			"query": map[string]any{
				"match_all": map[string]any{},
			},
		}

		size := 10
		resp, err := deps.Client.Search(ctx, &esclient.SearchRequest{
			Index:              indexName,
			Query:              query,
			CompanyID:          "test-company",
			Size:               &size,
			WithTrackTotalHits: true,
		})

		require.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Equal(t, 0, resp.Hits.Total.Value)
		assert.Len(t, resp.Hits.Hits, 0)
	})

	t.Run("count_documents", func(t *testing.T) {
		indexName := "test_count"
		deps.createTestIndex(t, indexName)

		// Count documents (should be 0)
		resp, err := deps.Client.Count(ctx, &esclient.CountRequest{
			Index:     indexName,
			CompanyID: "test-company",
		})

		require.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Equal(t, 0, resp.Count)
	})

	t.Run("point_in_time", func(t *testing.T) {
		indexName := "test_pit"
		deps.createTestIndex(t, indexName)

		// Open PIT
		pit, err := deps.Client.OpenPIT(ctx, &esclient.OpenPITRequest{
			Index:     indexName,
			KeepAlive: "1m",
		})
		require.NoError(t, err)
		assert.NotEmpty(t, pit.ID)

		// Close PIT
		err = deps.Client.ClosePIT(ctx, pit.ID)
		require.NoError(t, err)
	})

	t.Run("delete_index", func(t *testing.T) {
		indexName := "test_delete_index"
		deps.createTestIndex(t, indexName)

		// Verify exists
		exists, err := deps.Client.IndexExists(ctx, indexName)
		require.NoError(t, err)
		assert.True(t, exists)

		// Delete
		err = deps.Client.DeleteIndex(ctx, indexName)
		require.NoError(t, err)

		// Verify deleted
		exists, err = deps.Client.IndexExists(ctx, indexName)
		require.NoError(t, err)
		assert.False(t, exists)
	})
}

func TestRegistryOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	t.Run("get_default_client", func(t *testing.T) {
		client, err := registry.Default()
		require.NoError(t, err)
		assert.NotNil(t, client)
	})

	t.Run("get_client_by_name", func(t *testing.T) {
		client, err := registry.GetClient("tier-gold")
		require.NoError(t, err)
		assert.NotNil(t, client)
	})

	t.Run("get_nonexistent_client", func(t *testing.T) {
		_, err := registry.GetClient("nonexistent")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("list_clusters", func(t *testing.T) {
		clusters := registry.ListClusters()
		assert.Len(t, clusters, 2)
		assert.Contains(t, clusters, "tier-gold")
		assert.Contains(t, clusters, "tier-silver")
	})

	t.Run("get_entry", func(t *testing.T) {
		entry, err := registry.GetEntry("tier-gold")
		require.NoError(t, err)
		assert.Equal(t, "tier-gold", entry.Name)
		assert.Equal(t, 9, entry.Version)
		assert.NotNil(t, entry.ES)
		assert.Equal(t, esV9Addr, entry.BaseURL)
	})
}

func TestSearchBodyFromMap(t *testing.T) {
	t.Skip("SearchBodyFromMap removed - queries now passed as map[string]any directly")
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  *esclient.Config
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid_config",
			config: &esclient.Config{
				DefaultCluster: "test",
				Clusters: map[string]esclient.ClusterConfig{
					"test": {
						Name:      "test",
						Version:   9,
						Addresses: []string{"http://localhost:9200"},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "empty_clusters",
			config: &esclient.Config{
				DefaultCluster: "test",
				Clusters:       map[string]esclient.ClusterConfig{},
			},
			wantErr: true,
			errMsg:  "empty",
		},
		{
			name: "no_default_cluster",
			config: &esclient.Config{
				DefaultCluster: "",
				Clusters: map[string]esclient.ClusterConfig{
					"test": {
						Name:      "test",
						Version:   9,
						Addresses: []string{"http://localhost:9200"},
					},
				},
			},
			wantErr: true,
			errMsg:  "default",
		},
		{
			name: "default_cluster_not_found",
			config: &esclient.Config{
				DefaultCluster: "missing",
				Clusters: map[string]esclient.ClusterConfig{
					"test": {
						Name:      "test",
						Version:   9,
						Addresses: []string{"http://localhost:9200"},
					},
				},
			},
			wantErr: true,
			errMsg:  "not found",
		},
		{
			name: "invalid_version",
			config: &esclient.Config{
				DefaultCluster: "test",
				Clusters: map[string]esclient.ClusterConfig{
					"test": {
						Name:      "test",
						Version:   7, // invalid
						Addresses: []string{"http://localhost:9200"},
					},
				},
			},
			wantErr: true,
			errMsg:  "version",
		},
		{
			name: "empty_addresses",
			config: &esclient.Config{
				DefaultCluster: "test",
				Clusters: map[string]esclient.ClusterConfig{
					"test": {
						Name:      "test",
						Version:   9,
						Addresses: []string{},
					},
				},
			},
			wantErr: true,
			errMsg:  "addresses",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
