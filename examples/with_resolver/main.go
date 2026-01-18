package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	esclient "github.com/billz-2/elasticsearch-cluster"
	"github.com/redis/go-redis/v9"
)

func main() {
	ctx := context.Background()

	// 1. Create multi-cluster configuration
	config := &esclient.Config{
		DefaultCluster: "tier-gold",
		Clusters: map[string]esclient.ClusterConfig{
			"tier-gold": {
				Name:      "tier-gold",
				Version:   9,
				Addresses: []string{"http://localhost:9200"},
				Username:  "elastic",
				Password:  "changeme",
			},
			"tier-silver": {
				Name:      "tier-silver",
				Version:   8,
				Addresses: []string{"http://localhost:9201"},
				Username:  "elastic",
				Password:  "changeme",
			},
		},
	}

	// 2. Create registry
	registry, err := esclient.NewRegistryFromConfig(config)
	if err != nil {
		log.Fatal(err)
	}

	// 3. Setup Redis client
	redisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	// 4. Create resolver with Redis cache
	resolver, err := esclient.NewResolver(esclient.ResolverConfig{
		Registry: registry,
		Redis:    redisClient,
		SyncURL:  "http://localhost:8080", // billz-elastic-sync-service URL
		CacheTTL: 24 * time.Hour,
	})
	if err != nil {
		log.Fatal(err)
	}

	// 5. Example: Resolve cluster/index for a company
	// In real app, resolver would call sync service to get cluster info
	// For demo, we'll use raw resolution to show the pattern

	companyID := "company_123"
	indexType := "orders"

	// This would normally:
	// 1. Check Redis cache
	// 2. If miss, call sync service HTTP API
	// 3. Cache result
	// 4. Return typed client + index name

	// For this example, let's use registry directly
	client, err := resolver.ResolveRaw(ctx, companyID, indexType)
	if err != nil {
		// If sync service is not running, use registry directly
		log.Printf("Resolver error (expected if sync service not running): %v", err)
		log.Println("Using registry directly instead...")

		esClient, err := registry.GetClient("tier-gold")
		if err != nil {
			log.Fatal(err)
		}

		entry, err := registry.GetEntry("tier-gold")
		if err != nil {
			log.Fatal(err)
		}

		typedClient, err := esclient.NewClient(esClient, entry.BaseURL)
		if err != nil {
			log.Fatal(err)
		}

		// Use typed client
		indexName := "orders_tier_gold"

		// Create test index
		mapping := map[string]any{
			"mappings": map[string]any{
				"properties": map[string]any{
					"company_id": map[string]any{"type": "keyword"},
					"order_id":   map[string]any{"type": "keyword"},
					"amount":     map[string]any{"type": "float"},
				},
			},
		}
		mappingBytes, _ := json.Marshal(mapping)

		err = typedClient.CreateIndex(ctx, &esclient.CreateIndexRequest{
			Index: indexName,
			Body:  bytes.NewReader(mappingBytes),
		})
		if err != nil {
			log.Printf("Create index error (may already exist): %v", err)
		}

		// Search
		query := map[string]any{
			"query": map[string]any{
				"term": map[string]any{
					"company_id": companyID,
				},
			},
		}

		size := 100
		resp, err := typedClient.Search(ctx, &esclient.SearchRequest{
			Index:              indexName,
			Query:              query,
			CompanyID:          companyID,
			Size:               &size,
			WithTrackTotalHits: true,
		})
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("Found %d orders for company %s\n", resp.Hits.Total.Value, companyID)

		// Cleanup
		_ = typedClient.DeleteIndex(ctx, indexName)
	} else {
		fmt.Printf("Resolved cluster: %s, index: %s\n", client.ClusterName, client.IndexName)
	}

	fmt.Println("Resolver example completed!")
}
