# Examples

This directory contains examples demonstrating how to use the elasticsearch-cluster library.

## Prerequisites

Start Elasticsearch and Redis containers:

```bash
cd examples
docker-compose up -d

# Wait for services to be ready (about 30 seconds)
docker-compose ps
```

Check that all services are healthy:
- Elasticsearch v9 (tier-gold): http://localhost:9200
- Elasticsearch v8 (tier-silver): http://localhost:9201
- Redis: localhost:6379

## Running Examples

### 1. Basic Example

Demonstrates basic operations with a single cluster:
- Creating index
- Searching documents
- Counting documents

```bash
cd basic
go run main.go
```

**Key concepts:**
- Config with single cluster
- Registry pattern for client management
- Typed operations (Search, Count, CreateIndex, DeleteIndex)

### 2. Multi-Cluster Example

Shows how to work with multiple clusters (different ES versions):
- ES v9 (tier-gold) on port 9200
- ES v8 (tier-silver) on port 9201

```bash
cd multicluster
go run main.go
```

**Key concepts:**
- Multi-cluster configuration
- Version-agnostic operations
- Registry with multiple entries
- Tier-based routing

### 3. Resolver Example

Demonstrates using the Resolver for dynamic cluster/index resolution:
- Redis caching
- Sync service integration pattern
- Company-based routing

```bash
cd with_resolver
go run main.go
```

**Key concepts:**
- Resolver with Redis cache
- Integration with sync service
- Cache TTL configuration
- Fallback to direct registry usage

**Note:** This example shows the resolver pattern but doesn't require the actual sync service to be running. In production, the resolver would call the billz-elastic-sync-service HTTP API to get cluster routing info.

## Cleanup

Stop and remove containers:

```bash
docker-compose down -v
```

## Example Structure

```
examples/
├── docker-compose.yml          # Infrastructure setup
├── README.md                   # This file
├── basic/
│   └── main.go                # Single cluster example
├── multicluster/
│   └── main.go                # Multiple clusters example
└── with_resolver/
    └── main.go                # Resolver with Redis cache
```

## Integration in Your Service

In a real Billz microservice, you would:

1. **Load config from environment variables:**
```go
config := &esclient.Config{
    DefaultCluster: os.Getenv("ES_DEFAULT_CLUSTER"),
    Clusters: map[string]esclient.ClusterConfig{
        "tier-gold": {
            Name:      "tier-gold",
            Version:   9,
            Addresses: strings.Split(os.Getenv("ES_TIER_GOLD_ADDRESSES"), ","),
            Username:  os.Getenv("ES_TIER_GOLD_USER"),
            Password:  os.Getenv("ES_TIER_GOLD_PASSWORD"),
        },
        // ... more tiers
    },
}
```

2. **Initialize once at service startup:**
```go
// In main.go or service initialization
registry, err := esclient.NewRegistryFromConfig(config)
resolver, err := esclient.NewResolver(esclient.ResolverConfig{
    Registry: registry,
    Redis:    redisClient,
    SyncURL:  os.Getenv("SYNC_SERVICE_URL"),
    CacheTTL: 24 * time.Hour,
})
```

3. **Use resolver in business logic:**
```go
func (s *OrderService) GetOrders(ctx context.Context, companyID string) ([]Order, error) {
    // Resolve cluster and index for this company
    client, indexName, err := s.resolver.Resolve(ctx, companyID, "orders")
    if err != nil {
        return nil, err
    }

    // Execute search
    resp, err := client.Search(ctx, &esclient.SearchRequest{
        Index: indexName,
        Body:  queryBody,
    })

    return parseOrders(resp.Hits.Hits), nil
}
```

## Troubleshooting

**Connection refused errors:**
- Make sure Docker containers are running: `docker-compose ps`
- Wait for health checks to pass (check logs: `docker-compose logs`)

**ES v9 not available yet:**
- Use ES v8.11.0 for both clusters in docker-compose.yml
- Update config to use version 8 for both tiers

**Port already in use:**
- Change ports in docker-compose.yml (e.g., 9202, 9203)
- Update example code to use new ports
