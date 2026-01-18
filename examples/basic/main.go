package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"

	esclient "github.com/billz-2/elasticsearch-cluster"
)

func main() {
	ctx := context.Background()

	// 1. Create configuration for multiple clusters
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
		},
	}

	// 2. Create registry with all pre-created clients
	registry, err := esclient.NewRegistryFromConfig(config)
	if err != nil {
		log.Fatal(err)
	}

	// 3. Get ES client from registry
	esClient, err := registry.Default()
	if err != nil {
		log.Fatal(err)
	}

	// 4. Get registry entry to access base URL
	entry, err := registry.GetEntry("tier-gold")
	if err != nil {
		log.Fatal(err)
	}

	// 5. Create typed client wrapper
	client, err := esclient.NewClient(esClient, entry.BaseURL)
	if err != nil {
		log.Fatal(err)
	}

	// 6. Create index
	indexName := "products"
	mapping := map[string]any{
		"mappings": map[string]any{
			"properties": map[string]any{
				"title": map[string]any{"type": "text"},
				"price": map[string]any{"type": "float"},
			},
		},
	}

	mappingBytes, _ := json.Marshal(mapping)
	err = client.CreateIndex(ctx, &esclient.CreateIndexRequest{
		Index: indexName,
		Body:  bytes.NewReader(mappingBytes),
	})
	if err != nil {
		log.Printf("Create index error (may already exist): %v", err)
	}

	// 7. Search
	query := map[string]any{
		"query": map[string]any{
			"match_all": map[string]any{},
		},
	}

	size := 10
	resp, err := client.Search(ctx, &esclient.SearchRequest{
		Index:              indexName,
		Query:              query,
		CompanyID:          "example-company-id", // For demonstration
		Size:               &size,
		WithTrackTotalHits: true,
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Found %d documents\n", resp.Hits.Total.Value)

	// 8. Count documents
	countResp, err := client.Count(ctx, &esclient.CountRequest{
		Index:     indexName,
		CompanyID: "example-company-id",
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Total count: %d\n", countResp.Count)

	// 9. Cleanup
	err = client.DeleteIndex(ctx, indexName)
	if err != nil {
		log.Printf("Delete index error: %v", err)
	}

	fmt.Println("Example completed successfully!")
}
