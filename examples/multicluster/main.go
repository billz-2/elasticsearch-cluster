package main

import (
	"context"
	"fmt"
	"log"

	esclient "github.com/billz-2/elasticsearch-cluster"
)

func main() {
	ctx := context.Background()

	// 1. Configure multiple clusters with different versions
	config := &esclient.Config{
		DefaultCluster: "tier-gold",
		Clusters: map[string]esclient.ClusterConfig{
			"tier-gold": {
				Name:      "tier-gold",
				Version:   9, // ES v9
				Addresses: []string{"http://localhost:9200"},
				Username:  "elastic",
				Password:  "changeme",
			},
			"tier-silver": {
				Name:      "tier-silver",
				Version:   8, // ES v8
				Addresses: []string{"http://localhost:9201"},
				Username:  "elastic",
				Password:  "changeme",
			},
		},
	}

	// 2. Create registry (all clients created once)
	registry, err := esclient.NewRegistryFromConfig(config)
	if err != nil {
		log.Fatal(err)
	}

	// 3. List all available clusters
	clusters := registry.ListClusters()
	fmt.Printf("Available clusters: %v\n", clusters)

	// 4. Work with Gold tier (ES v9)
	goldClient, err := registry.GetClient("tier-gold")
	if err != nil {
		log.Fatal(err)
	}

	goldEntry, err := registry.GetEntry("tier-gold")
	if err != nil {
		log.Fatal(err)
	}

	typedGold, err := esclient.NewClient(goldClient, goldEntry.BaseURL)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Gold tier: version=%d, url=%s\n", goldEntry.Version, goldEntry.BaseURL)

	// 5. Work with Silver tier (ES v8)
	silverClient, err := registry.GetClient("tier-silver")
	if err != nil {
		log.Printf("Silver tier not available: %v", err)
	} else {
		silverEntry, err := registry.GetEntry("tier-silver")
		if err != nil {
			log.Fatal(err)
		}

		typedSilver, err := esclient.NewClient(silverClient, silverEntry.BaseURL)
		if err != nil {
			log.Printf("Cannot create silver client: %v", err)
		} else {
			fmt.Printf("Silver tier: version=%d, url=%s\n", silverEntry.Version, silverEntry.BaseURL)

			// Create index in silver tier
			mapping, _ := esclient.SearchBodyFromMap(map[string]interface{}{
				"mappings": map[string]interface{}{
					"properties": map[string]interface{}{
						"tier":  map[string]string{"type": "keyword"},
						"title": map[string]string{"type": "text"},
					},
				},
			})

			err = typedSilver.CreateIndex(ctx, &esclient.CreateIndexRequest{
				Index: "products_silver",
				Body:  mapping,
			})
			if err != nil {
				log.Printf("Silver tier create index error: %v", err)
			} else {
				fmt.Println("Created index in silver tier")
				_ = typedSilver.DeleteIndex(ctx, "products_silver")
			}
		}
	}

	// 6. Create index in gold tier
	mapping, _ := esclient.SearchBodyFromMap(map[string]interface{}{
		"mappings": map[string]interface{}{
			"properties": map[string]interface{}{
				"tier":  map[string]string{"type": "keyword"},
				"title": map[string]string{"type": "text"},
			},
		},
	})

	err = typedGold.CreateIndex(ctx, &esclient.CreateIndexRequest{
		Index: "products_gold",
		Body:  mapping,
	})
	if err != nil {
		log.Printf("Gold tier create index error: %v", err)
	} else {
		fmt.Println("Created index in gold tier")

		// Search in gold tier
		query, _ := esclient.SearchBodyFromMap(map[string]interface{}{
			"query": map[string]interface{}{
				"match_all": map[string]interface{}{},
			},
		})

		size := 10
		resp, err := typedGold.Search(ctx, &esclient.SearchRequest{
			Index:              "products_gold",
			Body:               query,
			Size:               &size,
			WithTrackTotalHits: true,
		})
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("Gold tier search: found %d documents\n", resp.Hits.Total.Value)

		// Cleanup
		_ = typedGold.DeleteIndex(ctx, "products_gold")
	}

	fmt.Println("\nMulti-cluster example completed!")
	fmt.Println("This demonstrates:")
	fmt.Println("- Registry with multiple ES versions (v8 and v9)")
	fmt.Println("- Version-agnostic operations")
	fmt.Println("- Tier-based routing")
}
