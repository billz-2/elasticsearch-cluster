package esclient

// ClusterConfig defines configuration for a single Elasticsearch cluster.
type ClusterConfig struct {
	Name      string   // Cluster name (e.g., "tier-gold", "tier-silver")
	Version   int      // Elasticsearch version: 8 or 9
	Addresses []string // Cluster addresses (e.g., ["http://es-1:9200", "http://es-2:9200"])
	Username  string   // Authentication username
	Password  string   // Authentication password
}

// Config defines configuration for multiple Elasticsearch clusters.
type Config struct {
	DefaultCluster string                   // Name of the default cluster
	Clusters       map[string]ClusterConfig // Map of cluster_name -> ClusterConfig
}

// Validate checks if configuration is valid.
func (c *Config) Validate() error {
	if len(c.Clusters) == 0 {
		return ErrEmptyClusters
	}

	if c.DefaultCluster == "" {
		return ErrNoDefaultCluster
	}

	if _, ok := c.Clusters[c.DefaultCluster]; !ok {
		return ErrDefaultClusterNotFound
	}

	for name, cluster := range c.Clusters {
		if name == "" {
			return ErrEmptyClusterName
		}
		if len(cluster.Addresses) == 0 {
			return ErrEmptyClusterAddresses(name)
		}
		if cluster.Version != 8 && cluster.Version != 9 {
			return ErrInvalidESVersion(name, cluster.Version)
		}
	}

	return nil
}
