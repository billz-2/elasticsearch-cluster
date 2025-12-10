package elasticcluster

import (
	"fmt"

	elasticv8 "github.com/elastic/go-elasticsearch/v8"
	elasticv9 "github.com/elastic/go-elasticsearch/v9"
)

type ClusterConn struct {
	V8 ESClient
	V9 ESClient
}

// Resolver selects ESClient by cluster name/version.
type Resolver struct {
	connections map[string]ClusterConn
}

func NewResolver(connections map[string]ClusterConn) *Resolver {
	return &Resolver{connections: connections}
}

// NewResolverFromConfig builds Resolver from config elastic cluster credentials.
func NewResolverFromConfig(elasticClusterNameCredsMap map[string]ElasticClusterCreds) (*Resolver, error) {
	if elasticClusterNameCredsMap == nil {
		return nil, fmt.Errorf("es resolver: config is nil")
	}

	resolverConns := make(map[string]ClusterConn, len(elasticClusterNameCredsMap))
	for name, cluster := range elasticClusterNameCredsMap {
		clientV8, err := elasticv8.NewClient(elasticv8.Config{
			Addresses: cluster.Addresses,
			Username:  cluster.User,
			Password:  cluster.Password,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create ES v8 client for cluster %s: %w", name, err)
		}

		clientV9, err := elasticv9.NewClient(elasticv9.Config{
			Addresses: cluster.Addresses,
			Username:  cluster.User,
			Password:  cluster.Password,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create ES v9 client for cluster %s: %w", name, err)
		}

		resolverConns[name] = ClusterConn{
			V8: NewClientV8(clientV8),
			V9: NewClientV9(clientV9),
		}
	}

	return NewResolver(resolverConns), nil
}

func (r *Resolver) Get(clusterName string, version int) ESClient {
	conn, ok := r.connections[clusterName]
	if !ok {
		return errorClient{err: fmt.Errorf("es resolver: cluster %s not configured", clusterName)}
	}

	switch version {
	case ESVersion9:
		if conn.V9 == nil {
			return errorClient{err: fmt.Errorf("es resolver: cluster %s has no v9 client", clusterName)}
		}
		return conn.V9
	case ESVersion8:
		if conn.V8 == nil {
			return errorClient{err: fmt.Errorf("es resolver: cluster %s has no v8 client", clusterName)}
		}
		return conn.V8
	default:
		return errorClient{err: fmt.Errorf("es resolver: unsupported version %d for cluster %s", version, clusterName)}
	}
}
