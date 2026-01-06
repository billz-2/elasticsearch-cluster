package esclient

import (
	"net/url"

	elasticV8 "github.com/elastic/go-elasticsearch/v8"
	elasticV9 "github.com/elastic/go-elasticsearch/v9"
	"github.com/pkg/errors"
)

// Entry represents a registered Elasticsearch cluster with pre-created client.
type Entry struct {
	Name    string   // Cluster name
	Version int      // Elasticsearch version (8 or 9)
	BaseURL string   // Base URL for the cluster
	ES      ESClient // Pre-created ES client
}

// Registry manages multiple Elasticsearch clusters.
// All clients are created once during initialization.
type Registry struct {
	defaultName string
	byName      map[string]Entry
}

// NewRegistry creates a new empty registry.
func NewRegistry(defaultName string) *Registry {
	if defaultName == "" {
		defaultName = "default"
	}
	return &Registry{
		defaultName: defaultName,
		byName:      make(map[string]Entry),
	}
}

// NewRegistryFromConfig creates registry from configuration.
// All ES clients are created during initialization (one-time setup).
func NewRegistryFromConfig(cfg *Config) (*Registry, error) {
	if err := cfg.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	reg := NewRegistry(cfg.DefaultCluster)

	for name, clusterCfg := range cfg.Clusters {
		// Parse and validate base URL
		baseURL := clusterCfg.Addresses[0]
		u, err := url.Parse(baseURL)
		if err != nil || u.Scheme == "" || u.Host == "" {
			return nil, ErrInvalidBaseURL(name, baseURL)
		}

		var client ESClient

		// Create appropriate client based on version
		switch clusterCfg.Version {
		case 9:
			cl, err := elasticV9.NewClient(elasticV9.Config{
				Addresses: clusterCfg.Addresses,
				Username:  clusterCfg.Username,
				Password:  clusterCfg.Password,
			})
			if err != nil {
				return nil, errors.Wrapf(err, "failed to create ES v9 client for %q", name)
			}
			client = NewESClientV9(cl, u)

		case 8:
			cl, err := elasticV8.NewClient(elasticV8.Config{
				Addresses: clusterCfg.Addresses,
				Username:  clusterCfg.Username,
				Password:  clusterCfg.Password,
			})
			if err != nil {
				return nil, errors.Wrapf(err, "failed to create ES v8 client for %q", name)
			}
			client = NewESClientV8(cl, u)

		default:
			// This should never happen after Validate()
			return nil, ErrInvalidESVersion(name, clusterCfg.Version)
		}

		reg.byName[name] = Entry{
			Name:    name,
			Version: clusterCfg.Version,
			BaseURL: baseURL,
			ES:      client,
		}
	}

	return reg, nil
}

// GetClient returns pre-created ES client by cluster name.
// Returns error if cluster not found.
func (r *Registry) GetClient(clusterName string) (ESClient, error) {
	if clusterName == "" {
		clusterName = r.defaultName
	}

	entry, ok := r.byName[clusterName]
	if !ok {
		return nil, ErrClusterNotFound(clusterName)
	}

	return entry.ES, nil
}

// GetEntry returns full entry (client + metadata) by cluster name.
func (r *Registry) GetEntry(clusterName string) (Entry, error) {
	if clusterName == "" {
		clusterName = r.defaultName
	}

	entry, ok := r.byName[clusterName]
	if !ok {
		return Entry{}, ErrClusterNotFound(clusterName)
	}

	return entry, nil
}

// Default returns the default cluster client.
func (r *Registry) Default() (ESClient, error) {
	return r.GetClient(r.defaultName)
}

// ListClusters returns list of all registered cluster names.
func (r *Registry) ListClusters() []string {
	names := make([]string, 0, len(r.byName))
	for name := range r.byName {
		names = append(names, name)
	}
	return names
}
