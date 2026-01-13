package e2e

import (
	"testing"

	esclient "github.com/billz-2/elasticsearch-cluster"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMultiClusterOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	t.Run("list_all_clusters", func(t *testing.T) {
		clusters := registry.ListClusters()
		assert.Len(t, clusters, 2)
		assert.Contains(t, clusters, "tier-gold")
		assert.Contains(t, clusters, "tier-silver")
	})

	t.Run("get_gold_tier_client", func(t *testing.T) {
		client, err := registry.GetClient("tier-gold")
		require.NoError(t, err)
		assert.NotNil(t, client)

		// Verify entry details
		entry, err := registry.GetEntry("tier-gold")
		require.NoError(t, err)
		assert.Equal(t, "tier-gold", entry.Name)
		assert.Equal(t, 9, entry.Version)
		assert.Equal(t, esV9Addr, entry.BaseURL)
	})

	t.Run("get_silver_tier_client", func(t *testing.T) {
		client, err := registry.GetClient("tier-silver")
		require.NoError(t, err)
		assert.NotNil(t, client)

		// Verify entry details
		entry, err := registry.GetEntry("tier-silver")
		require.NoError(t, err)
		assert.Equal(t, "tier-silver", entry.Name)
		assert.Equal(t, 8, entry.Version)
		assert.Equal(t, esV8Addr, entry.BaseURL)
	})

	t.Run("default_cluster", func(t *testing.T) {
		client, err := registry.Default()
		require.NoError(t, err)
		assert.NotNil(t, client)

		// Should return tier-gold as it's the default
		entry, err := registry.GetEntry("tier-gold")
		require.NoError(t, err)
		assert.Equal(t, "tier-gold", entry.Name)
	})

	t.Run("create_index_in_both_clusters", func(t *testing.T) {
		indexName := "test_multicluster"

		// Create index in gold tier (v9)
		goldClient, err := registry.GetClient("tier-gold")
		require.NoError(t, err)

		typedGold, err := esclient.NewClient(goldClient, esV9Addr)
		require.NoError(t, err)

		mapping, _ := esclient.SearchBodyFromMap(map[string]interface{}{
			"mappings": map[string]interface{}{
				"properties": map[string]interface{}{
					"tier": map[string]string{"type": "keyword"},
				},
			},
		})

		err = typedGold.CreateIndex(ctx, &esclient.CreateIndexRequest{
			Index: indexName,
			Body:  mapping,
		})
		require.NoError(t, err)

		// Verify index exists in gold
		exists, err := typedGold.IndexExists(ctx, indexName)
		require.NoError(t, err)
		assert.True(t, exists)

		// Create index in silver tier (v8)
		silverClient, err := registry.GetClient("tier-silver")
		require.NoError(t, err)

		typedSilver, err := esclient.NewClient(silverClient, esV8Addr)
		require.NoError(t, err)

		err = typedSilver.CreateIndex(ctx, &esclient.CreateIndexRequest{
			Index: indexName,
			Body:  mapping,
		})
		require.NoError(t, err)

		// Verify index exists in silver
		exists, err = typedSilver.IndexExists(ctx, indexName)
		require.NoError(t, err)
		assert.True(t, exists)

		// Cleanup both
		_ = typedGold.DeleteIndex(ctx, indexName)
		_ = typedSilver.DeleteIndex(ctx, indexName)
	})

	t.Run("search_in_both_clusters", func(t *testing.T) {
		indexName := "test_search_multicluster"

		// Setup gold tier
		goldClient, err := registry.GetClient("tier-gold")
		require.NoError(t, err)
		typedGold, err := esclient.NewClient(goldClient, esV9Addr)
		require.NoError(t, err)

		mapping, _ := esclient.SearchBodyFromMap(map[string]interface{}{
			"mappings": map[string]interface{}{
				"properties": map[string]interface{}{
					"title": map[string]string{"type": "text"},
				},
			},
		})
		_ = typedGold.CreateIndex(ctx, &esclient.CreateIndexRequest{Index: indexName, Body: mapping})

		// Setup silver tier
		silverClient, err := registry.GetClient("tier-silver")
		require.NoError(t, err)
		typedSilver, err := esclient.NewClient(silverClient, esV8Addr)
		require.NoError(t, err)
		_ = typedSilver.CreateIndex(ctx, &esclient.CreateIndexRequest{Index: indexName, Body: mapping})

		// Search in gold
		query, _ := esclient.SearchBodyFromMap(map[string]interface{}{
			"query": map[string]interface{}{"match_all": map[string]interface{}{}},
		})
		size := 10

		goldResp, err := typedGold.Search(ctx, &esclient.SearchRequest{
			Index: indexName,
			Body:  query,
			Size:  &size,
		})
		require.NoError(t, err)
		assert.Equal(t, 0, goldResp.Hits.Total.Value)

		// Search in silver
		silverResp, err := typedSilver.Search(ctx, &esclient.SearchRequest{
			Index: indexName,
			Body:  query,
			Size:  &size,
		})
		require.NoError(t, err)
		assert.Equal(t, 0, silverResp.Hits.Total.Value)

		// Cleanup
		_ = typedGold.DeleteIndex(ctx, indexName)
		_ = typedSilver.DeleteIndex(ctx, indexName)
	})
}

func TestMultiClusterConfig(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	t.Run("create_registry_with_multiple_versions", func(t *testing.T) {
		// This test verifies that config validation works
		// for multiple clusters with different versions
		config := &esclient.Config{
			DefaultCluster: "v9-cluster",
			Clusters: map[string]esclient.ClusterConfig{
				"v9-cluster": {
					Name:      "v9-cluster",
					Version:   9,
					Addresses: []string{"http://localhost:9200"},
				},
				"v8-cluster": {
					Name:      "v8-cluster",
					Version:   8,
					Addresses: []string{"http://localhost:9201"},
				},
			},
		}

		err := config.Validate()
		require.NoError(t, err)
	})

	t.Run("registry_prevents_empty_cluster_name", func(t *testing.T) {
		// Verify we can't create config with empty cluster name (key)
		config := &esclient.Config{
			DefaultCluster: "valid-cluster",
			Clusters: map[string]esclient.ClusterConfig{
				"": { // empty key
					Name:      "cluster1",
					Version:   9,
					Addresses: []string{esV9Addr},
				},
				"valid-cluster": {
					Name:      "valid-cluster",
					Version:   9,
					Addresses: []string{esV9Addr},
				},
			},
		}

		err := config.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cluster name is empty")
	})
}
