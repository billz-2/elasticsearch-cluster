package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	esclient "github.com/billz-2/elasticsearch-cluster"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/elasticsearch"
	rediscontainer "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	ctx context.Context

	// ES v9 (tier-gold) resources
	esV9Container *elasticsearch.ElasticsearchContainer
	esV9Addr      string

	// ES v8 (tier-silver) resources - for multi-cluster tests
	esV8Container *elasticsearch.ElasticsearchContainer
	esV8Addr      string

	// Redis resources
	redisContainer *rediscontainer.RedisContainer
	redisAddr      string
	redisClient    *redis.Client

	// Registry with both clusters
	registry *esclient.Registry

	// Resolver
	resolver *esclient.Resolver
)

func TestMain(m *testing.M) {
	ctx = context.Background()

	// Setup ES v9 (primary cluster for most tests)
	var err error
	esV9Container, err = elasticsearch.Run(ctx,
		"docker.elastic.co/elasticsearch/elasticsearch:9.0.0",
		elasticsearch.WithPassword("changeme"),
		testcontainers.WithEnv(map[string]string{
			"discovery.type":         "single-node",
			"xpack.security.enabled": "false",
		}),
		testcontainers.WithWaitStrategy(
			wait.ForLog("started").
				WithStartupTimeout(2*time.Minute).
				WithPollInterval(1*time.Second),
		),
	)
	if err != nil {
		panic(err)
	}

	esV9Addr, err = esV9Container.Endpoint(ctx, "http")
	if err != nil {
		panic(err)
	}

	// Setup ES v8 (for multi-cluster tests)
	esV8Container, err = elasticsearch.Run(ctx,
		"docker.elastic.co/elasticsearch/elasticsearch:8.11.0",
		elasticsearch.WithPassword("changeme"),
		testcontainers.WithEnv(map[string]string{
			"discovery.type":         "single-node",
			"xpack.security.enabled": "false",
		}),
		testcontainers.WithWaitStrategy(
			wait.ForLog("started").
				WithStartupTimeout(2*time.Minute).
				WithPollInterval(1*time.Second),
		),
	)
	if err != nil {
		panic(err)
	}

	esV8Addr, err = esV8Container.Endpoint(ctx, "http")
	if err != nil {
		panic(err)
	}

	// Setup Redis
	redisContainer, err = rediscontainer.Run(ctx,
		"redis:7-alpine",
		testcontainers.WithWaitStrategy(
			wait.ForLog("Ready to accept connections").
				WithStartupTimeout(30*time.Second).
				WithPollInterval(500*time.Millisecond),
		),
	)
	if err != nil {
		panic(err)
	}

	redisAddr, err = redisContainer.Endpoint(ctx, "")
	if err != nil {
		panic(err)
	}

	options, err := redis.ParseURL("redis://" + redisAddr)
	if err != nil {
		panic(err)
	}
	redisClient = redis.NewClient(options)

	// Create multi-cluster registry
	config := &esclient.Config{
		DefaultCluster: "tier-gold",
		Clusters: map[string]esclient.ClusterConfig{
			"tier-gold": {
				Name:      "tier-gold",
				Version:   9,
				Addresses: []string{esV9Addr},
				Username:  "elastic",
				Password:  "changeme",
			},
			"tier-silver": {
				Name:      "tier-silver",
				Version:   8,
				Addresses: []string{esV8Addr},
				Username:  "elastic",
				Password:  "changeme",
			},
		},
	}

	registry, err = esclient.NewRegistryFromConfig(config)
	if err != nil {
		panic(err)
	}

	// Create resolver
	resolver, err = esclient.NewResolver(esclient.ResolverConfig{
		Registry: registry,
		Redis:    redisClient,
		SyncURL:  "http://localhost:8080", // Mock URL for tests
		CacheTTL: 24 * time.Hour,
	})
	if err != nil {
		panic(err)
	}

	// Run tests
	code := m.Run()

	// Cleanup
	if redisClient != nil {
		_ = redisClient.FlushAll(ctx).Err()
		_ = redisClient.Close()
	}
	if redisContainer != nil {
		_ = redisContainer.Terminate(ctx)
	}
	if esV9Container != nil {
		_ = esV9Container.Terminate(ctx)
	}
	if esV8Container != nil {
		_ = esV8Container.Terminate(ctx)
	}

	os.Exit(code)
}

// testDeps contains per-test dependencies
type testDeps struct {
	Client   *esclient.Client
	Registry *esclient.Registry
	Resolver *esclient.Resolver
	Redis    *redis.Client
	ESAddr   string
	ESV8Addr string
}

// newTestDeps creates test dependencies for a specific cluster
func newTestDeps(t *testing.T, clusterName string) *testDeps {
	t.Helper()

	esClient, err := registry.GetClient(clusterName)
	if err != nil {
		t.Fatalf("failed to get ES client: %v", err)
	}

	entry, err := registry.GetEntry(clusterName)
	if err != nil {
		t.Fatalf("failed to get registry entry: %v", err)
	}

	client, err := esclient.NewClient(esClient, entry.BaseURL)
	if err != nil {
		t.Fatalf("failed to create typed client: %v", err)
	}

	return &testDeps{
		Client:   client,
		Registry: registry,
		Resolver: resolver,
		Redis:    redisClient,
		ESAddr:   esV9Addr,
		ESV8Addr: esV8Addr,
	}
}

// createTestIndex is a helper to create test indices
func (td *testDeps) createTestIndex(t *testing.T, indexName string) {
	t.Helper()

	exists, err := td.Client.IndexExists(ctx, indexName)
	if err != nil {
		t.Fatalf("failed to check index existence: %v", err)
	}

	if exists {
		return
	}

	mapping := map[string]any{
		"mappings": map[string]any{
			"properties": map[string]any{
				"title":      map[string]any{"type": "text"},
				"price":      map[string]any{"type": "float"},
				"company_id": map[string]any{"type": "keyword"},
				"created_at": map[string]any{"type": "date"},
			},
		},
	}
	mappingBytes, _ := json.Marshal(mapping)

	err = td.Client.CreateIndex(ctx, &esclient.CreateIndexRequest{
		Index: indexName,
		Body:  bytes.NewReader(mappingBytes),
	})
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	// Cleanup on test completion
	t.Cleanup(func() {
		_ = td.Client.DeleteIndex(ctx, indexName)
	})
}
