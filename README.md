# Elasticsearch Cluster Client

Universal Elasticsearch client library for Billz microservices with multi-cluster support, version abstraction (ES v8/v9), and tier-based routing via Redis cache.

## Features

- ✅ **Multi-cluster support** - Manage multiple ES clusters from single config
- ✅ **Version-agnostic** - Unified interface for ES v8 and v9
- ✅ **HTTP transport abstraction** - No breaking changes from ES client updates
- ✅ **Pre-created clients** - All clients initialized once at startup
- ✅ **Tier-based routing** - Resolve cluster/index via Redis cache + sync service
- ✅ **Typed operations** - Convenient methods for Search, Bulk, PIT, etc.
- ✅ **Zero duplication** - Single source of truth for cluster configuration
- ✅ **E2E tested** - Comprehensive test coverage with testcontainers

## Architecture

```
┌─────────────────┐
│   Service       │
└────────┬────────┘
         │
         ▼
┌─────────────────┐      ┌──────────────┐
│   Resolver      │─────▶│ Redis Cache  │
└────────┬────────┘      └──────────────┘
         │                     │
         │ (cache miss)        │
         ▼                     │
┌─────────────────┐            │
│  Sync Service   │◀───────────┘
└────────┬────────┘
         │ {cluster_name, index_name}
         ▼
┌─────────────────┐
│    Registry     │  (map lookup)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  ESClient (v8)  │
│  ESClient (v9)  │
└─────────────────┘
```

## Installation

```bash
go get github.com/billz-2/elasticsearch-cluster
```

## Publishing New Version

### Official Release (from master)

1. Merge staging → master
2. Checkout master and create release:
   ```bash
   make release VERSION=v0.0.29
   ```
3. Wait up to 30 minutes for Go proxy to update
4. Update in microservices: `go get -u github.com/billz-2/elasticsearch-cluster@v0.0.29`

### Development/Testing Tags (from feature branch)

If you need to test changes before merging to master:

```bash
# From your feature branch
git tag -a v0.0.29-dev -m "v0.0.29-dev: testing new feature"
git push origin v0.0.29-dev

# In microservice, test with:
go get github.com/billz-2/elasticsearch-cluster@v0.0.29-dev
```

**Note:** Use semantic versioning with suffixes for non-release tags (e.g., `-dev`, `-alpha`, `-beta`)

## Quick Commands

```bash
# Build library
make build

# Run unit tests (fast)
make test-unit

# Run E2E tests with testcontainers
make test-e2e

# Run all tests
make test

# Run CI pipeline
make ci

# Show all commands
make help
```

## Examples

See working examples in the [`examples/`](examples/) directory:

```bash
# Start infrastructure (ES v8, ES v9, Redis)
cd examples
docker-compose up -d

# Run examples
cd basic && go run main.go
cd multicluster && go run main.go
cd with_resolver && go run main.go
```

See [examples/README.md](examples/README.md) for detailed documentation.

## Quick Start

### 1. Initialize Registry (at service startup)

```go
package main

import (
    "log"
    "os"

    esclient "github.com/billz-2/elasticsearch-cluster"
    "github.com/redis/go-redis/v9"
)

func main() {
    // Config from environment variables (vault)
    config := &esclient.Config{
        DefaultCluster: "tier-gold",
        Clusters: map[string]esclient.ClusterConfig{
            "tier-gold": {
                Name:      "tier-gold",
                Version:   9,
                Addresses: []string{"http://es-gold-1:9200", "http://es-gold-2:9200"},
                Username:  os.Getenv("ES_TIER_GOLD_USER"),
                Password:  os.Getenv("ES_TIER_GOLD_PASSWORD"),
            },
            "tier-silver": {
                Name:      "tier-silver",
                Version:   8,
                Addresses: []string{"http://es-silver:9200"},
                Username:  os.Getenv("ES_TIER_SILVER_USER"),
                Password:  os.Getenv("ES_TIER_SILVER_PASSWORD"),
            },
        },
    }

    // Create registry with all pre-created clients
    registry, err := esclient.NewRegistryFromConfig(config)
    if err != nil {
        log.Fatal(err)
    }

    // Create resolver with Redis cache
    redisClient := redis.NewClient(&redis.Options{
        Addr: "localhost:6379",
    })

    resolver, err := esclient.NewResolver(esclient.ResolverConfig{
        Registry: registry,
        Redis:    redisClient,
        SyncURL:  "http://sync-service:8080",
        CacheTTL: 24 * time.Hour,
    })
    if err != nil {
        log.Fatal(err)
    }

    // Use resolver in your service
    startService(resolver)
}
```

### 2. Use Resolver in Business Logic

```go
func SearchOrders(ctx context.Context, resolver *esclient.Resolver, companyID string) ([]Order, error) {
    // Resolve cluster and index for this company
    client, indexName, err := resolver.Resolve(ctx, companyID, "orders")
    if err != nil {
        return nil, err
    }

    // Build search query (structured map, not bytes)
    query := map[string]any{
        "query": map[string]any{
            "range": map[string]any{
                "created_at": map[string]any{"gte": "now-7d"},
            },
        },
        "sort": []map[string]any{{"created_at": map[string]any{"order": "desc"}}},
    }

    // Execute search
    // company_id filter is automatically injected for shared indices
    size := 100
    resp, err := client.Search(ctx, &esclient.SearchRequest{
        Index:              indexName,
        Query:              query,
        CompanyID:          companyID,  // Required for shared indices
        Size:               &size,
        WithTrackTotalHits: true,
    })
    if err != nil {
        return nil, err
    }

    // Parse results
    orders := parseOrders(resp.Hits.Hits)
    return orders, nil
}
```

### 3. Direct Registry Usage (without resolver)

```go
// Get default cluster client
esClient, err := registry.Default()
if err != nil {
    return err
}

// Or get specific cluster
esClient, err := registry.GetClient("tier-gold")
if err != nil {
    return err
}

// Create typed client wrapper
client, err := esclient.NewClient(esClient, "http://es-gold-1:9200")
if err != nil {
    return err
}

// Use typed operations
resp, err := client.Search(ctx, &esclient.SearchRequest{
    Index:     "orders_tier_gold",
    Query:     queryMap,
    CompanyID: companyID,
})
```

## Debug Logging

The library supports optional debug logging to trace Elasticsearch requests and resolver decisions. This is especially useful for debugging routing issues in microservices.

### Features

- **No-op by default**: If logger is not provided, all logging is disabled (zero overhead)
- **Compatible with billz logger**: Works with `github.com/billz-2/packages/pkg/logger`
- **Debug-only**: Logs only Debug level (no errors - errors are returned as usual)
- **Safe for nil**: All logging calls are safe when logger is nil

### Usage with Logger

```go
import (
    esclient "github.com/billz-2/elasticsearch-cluster"
    "github.com/billz-2/packages/pkg/logger"
)

// Initialize logger
log := logger.New(logger.LevelDebug, "billz_catalog_service_v2")

// Create registry with logger
registry, err := esclient.NewRegistryFromConfigWithLogger(config, log)
if err != nil {
    log.Fatal("failed to create ES registry", logger.Error(err))
}

// Create resolver with logger
resolver, err := esclient.NewResolver(esclient.ResolverConfig{
    Registry: registry,
    Redis:    redisClient,
    SyncURL:  "http://sync-service:8080",
    CacheTTL: 24 * time.Hour,
    Logger:   log,  // Pass logger here
})
```

### What Gets Logged

The library logs:

1. **Resolver decisions**:
   - Cache hits/misses
   - Sync service calls
   - Default cluster fallback (for non-migrated indices)

2. **Low-level HTTP requests**:
   - Method, path, host
   - Response status codes

### Example Logs

```
[DEBUG] [trace:abc123] elasticsearch resolver resolve company_id=comp_001 index_type=products
[DEBUG] [trace:abc123] elasticsearch resolver cache miss
[DEBUG] [trace:abc123] elasticsearch resolver resolved from sync cluster_name=tier-gold index_name=products_tier_gold_comp_001
[DEBUG] [trace:abc123] elasticsearch request method=POST path=/products_tier_gold_comp_001/_search host=es-gold-1:9200
[DEBUG] [trace:abc123] elasticsearch response status_code=200 path=/products_tier_gold_comp_001/_search
```

### Logger Interface

The library uses a minimal logger interface compatible with billz services:

```go
type Logger interface {
    Debug(msg string, fields ...Field)
    DebugWithCtx(ctx context.Context, msg string, fields ...Field)
}

type Field interface{}  // Compatible with zapcore.Field
```

### Custom Logger Example

```go
type MyLogger struct{}

func (l *MyLogger) Debug(msg string, fields ...interface{}) {
    fmt.Printf("[DEBUG] %s %+v\n", msg, fields)
}

func (l *MyLogger) DebugWithCtx(ctx context.Context, msg string, fields ...interface{}) {
    traceID := ctx.Value("trace_id")
    fmt.Printf("[DEBUG] [trace:%v] %s %+v\n", traceID, msg, fields)
}

// Use custom logger
log := &MyLogger{}
registry, _ := esclient.NewRegistryFromConfigWithLogger(config, log)
```

## API Reference

### Registry

```go
// Create from config
registry, err := esclient.NewRegistryFromConfig(config)

// Get client by name
client, err := registry.GetClient("tier-gold")

// Get default client
client, err := registry.Default()

// Get full entry with metadata
entry, err := registry.GetEntry("tier-gold")
// entry.Name, entry.Version, entry.BaseURL, entry.ES

// List all clusters
names := registry.ListClusters()
```

### Resolver

```go
// Resolve cluster and index for company
client, indexName, err := resolver.Resolve(ctx, companyID, "orders")

// Get raw cluster info (without creating client)
info, err := resolver.ResolveRaw(ctx, companyID, "products")
// info.ClusterName, info.ClusterID, info.IndexName

// Invalidate cache for specific company + index type
err := resolver.InvalidateCache(ctx, companyID, "orders")

// Invalidate all cache for company
err := resolver.InvalidateCompanyCache(ctx, companyID)
```

### Typed Client Operations

```go
// Search with automatic company_id filter injection for shared indices
resp, err := client.Search(ctx, &esclient.SearchRequest{
    Index:              "orders",
    Query:              queryMap,           // structured query
    CompanyID:          companyID,          // required for shared indices
    Size:               &size,
    From:               &offset,
    WithTrackTotalHits: true,
})

// Bulk operations (no query, no company filter needed)
resp, err := client.Bulk(ctx, &esclient.BulkRequest{
    Index: "orders",
    Body:  bulkBody, // NDJSON format
})

// Point-in-time pagination
pit, err := client.OpenPIT(ctx, &esclient.OpenPITRequest{
    Index:     "orders",
    KeepAlive: "5m",
})
defer client.ClosePIT(ctx, pit.ID)

// Delete by query with automatic company_id filter
resp, err := client.DeleteByQuery(ctx, &esclient.DeleteByQueryRequest{
    Index:     "orders",
    Query:     deleteQuery,  // structured query
    CompanyID: companyID,    // required for shared indices
})

// Index management (no query)
err := client.CreateIndex(ctx, &esclient.CreateIndexRequest{
    Index: "new_index",
    Body:  mappingsBody,
})

exists, err := client.IndexExists(ctx, "orders")

err := client.DeleteIndex(ctx, "old_index")

// Count documents with automatic company_id filter
resp, err := client.Count(ctx, &esclient.CountRequest{
    Index:     "orders",
    Query:     countQuery,  // optional, structured query
    CompanyID: companyID,   // required for shared indices
})

// Update by query with automatic company_id filter
resp, err := client.UpdateByQuery(ctx, &esclient.UpdateByQueryRequest{
    Index:     "orders",
    Query:     updateScript,  // structured query with script
    CompanyID: companyID,     // required for shared indices
})
```

## Configuration

### Environment Variables Pattern

```bash
# Tier Gold Cluster (ES v9)
ES_TIER_GOLD_ADDRESSES=http://es-gold-1:9200,http://es-gold-2:9200
ES_TIER_GOLD_USER=elastic
ES_TIER_GOLD_PASSWORD=secret123
ES_TIER_GOLD_VERSION=9

# Tier Silver Cluster (ES v8)
ES_TIER_SILVER_ADDRESSES=http://es-silver:9200
ES_TIER_SILVER_USER=elastic
ES_TIER_SILVER_PASSWORD=secret456
ES_TIER_SILVER_VERSION=8

# Resolver settings
SYNC_SERVICE_URL=http://billz-elastic-sync-service:8080
REDIS_ADDR=redis:6379
ES_CACHE_TTL=24h
```

### Config Structure

```go
type Config struct {
    DefaultCluster string
    Clusters       map[string]ClusterConfig
}

type ClusterConfig struct {
    Name      string   // e.g., "tier-gold"
    Version   int      // 8 or 9
    Addresses []string
    Username  string
    Password  string
}
```

## Testing

The library uses testcontainers for E2E testing. Tests automatically start Elasticsearch and Redis containers.

```bash
# Run all tests (unit + e2e)
make test

# Run only unit tests (fast, no containers)
make test-unit

# Run only E2E tests (with containers)
make test-e2e

# Run tests with coverage
make test-coverage
```

### Example E2E Test

```go
func TestBasicOperations(t *testing.T) {
    ctx := context.Background()
    env := e2e.SetupTestEnv(t, ctx)
    client := env.GetClient(t)

    // Create index
    err := client.CreateIndex(ctx, &esclient.CreateIndexRequest{
        Index: "test_index",
        Body:  mapping,
    })
    require.NoError(t, err)

    // Search
    resp, err := client.Search(ctx, &esclient.SearchRequest{
        Index: "test_index",
        Body:  query,
    })
    require.NoError(t, err)

    // Cleanup handled automatically by t.Cleanup()
}
```

## Migration from Old Code

### Before (duplicated in each service)

```go
// Each service had its own cluster management
switch clusterInfo.Version {
case config.ESVersion9:
    res, err := clusterInfo.ESClientV9.Search(...)
default:
    res, err := clusterInfo.ESClientV8.Search(...)
}
```

### After (unified library)

```go
// Simple, clean, unified - company_id filter automatically injected for shared indices
client, indexName, err := resolver.Resolve(ctx, companyID, "orders")
resp, err := client.Search(ctx, &esclient.SearchRequest{
    Index:     indexName,
    Query:     queryMap,
    CompanyID: companyID,
})
```

## Shared Index Support

The library automatically handles per-company and shared indices:

- **Per-company indices**: `products_<company-uuid>` - no company_id filter added
- **Shared indices**: `products_shared` - automatically injects `term: {company_id: <uuid>}` filter

### How It Works

1. **Index detection**: Library detects index type by name pattern (UUID suffix = per-company)
2. **Query mutation**: For shared indices, adds company_id filter to `bool.filter` array
3. **Safe injection**: Deep copies query to avoid mutating caller's data
4. **Schema agnostic**: Works with both `company_id` (keyword) and `company_id.keyword` (text+keyword)

### Example

```go
// Service code - same for both index types
query := map[string]any{
    "query": map[string]any{
        "match": map[string]any{"title": "laptop"},
    },
}

resp, err := client.Search(ctx, &esclient.SearchRequest{
    Index:     indexName,  // "products_<uuid>" OR "products_shared"
    Query:     query,
    CompanyID: companyID,
})

// For per-company index: query sent as-is
// For shared index: automatically becomes:
// {
//   "query": {
//     "bool": {
//       "must": [{"match": {"title": "laptop"}}],
//       "filter": [{"term": {"company_id": "<uuid>"}}]
//     }
//   }
// }
```
