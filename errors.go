package esclient

import "fmt"

// Configuration errors
var (
	ErrEmptyClusters          = fmt.Errorf("clusters map is empty")
	ErrNoDefaultCluster       = fmt.Errorf("default cluster name not specified")
	ErrDefaultClusterNotFound = fmt.Errorf("default cluster not found in clusters map")
	ErrEmptyClusterName       = fmt.Errorf("cluster name is empty")
)

// ErrEmptyClusterAddresses returns error for cluster with no addresses.
func ErrEmptyClusterAddresses(clusterName string) error {
	return fmt.Errorf("cluster %q has no addresses", clusterName)
}

// ErrInvalidESVersion returns error for unsupported ES version.
func ErrInvalidESVersion(clusterName string, version int) error {
	return fmt.Errorf("cluster %q has invalid ES version %d (must be 8 or 9)", clusterName, version)
}

// ErrClusterNotFound returns error when cluster is not found in registry.
func ErrClusterNotFound(clusterName string) error {
	return fmt.Errorf("cluster %q not found in registry", clusterName)
}

// ErrInvalidBaseURL returns error for invalid cluster base URL.
func ErrInvalidBaseURL(clusterName, address string) error {
	return fmt.Errorf("cluster %q has invalid base URL %q (must be absolute URL)", clusterName, address)
}

type StatusError struct {
	Op         string
	StatusCode int
}

func (e *StatusError) Error() string {
	if e.Op == "" {
		return fmt.Sprintf("elasticsearch returned status %d", e.StatusCode)
	}
	return fmt.Sprintf("%s returned status code %d", e.Op, e.StatusCode)
}
