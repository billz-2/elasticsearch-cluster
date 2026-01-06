package esclient

import "io"

// SearchRequest represents Elasticsearch search request.
type SearchRequest struct {
	Index              string      // Index name or pattern
	Body               io.Reader   // Query body (JSON)
	Size               *int        // Number of results to return
	From               *int        // Offset for pagination
	WithTrackTotalHits bool        // Track total hits accurately
	PointInTime        *string     // Point-in-time ID for pagination
	SearchAfter        interface{} // Search after values for pagination
}

// SearchResponse represents Elasticsearch search response.
type SearchResponse struct {
	Took     int                    `json:"took"`
	TimedOut bool                   `json:"timed_out"`
	Shards   map[string]interface{} `json:"_shards"`
	Hits     struct {
		Total struct {
			Value    int    `json:"value"`
			Relation string `json:"relation"`
		} `json:"total"`
		MaxScore *float64                 `json:"max_score"`
		Hits     []map[string]interface{} `json:"hits"`
	} `json:"hits"`
	PitID string `json:"pit_id,omitempty"`
}

// BulkRequest represents Elasticsearch bulk request.
type BulkRequest struct {
	Index string    // Default index name
	Body  io.Reader // Bulk operations body (NDJSON)
}

// BulkResponse represents Elasticsearch bulk response.
type BulkResponse struct {
	Took   int                      `json:"took"`
	Errors bool                     `json:"errors"`
	Items  []map[string]interface{} `json:"items"`
}

// OpenPITRequest represents open point-in-time request.
type OpenPITRequest struct {
	Index     string // Index name or pattern
	KeepAlive string // Keep alive duration (e.g., "1m")
}

// PIT represents point-in-time response.
type PIT struct {
	ID string `json:"id"`
}

// DeleteByQueryRequest represents delete by query request.
type DeleteByQueryRequest struct {
	Index string    // Index name
	Body  io.Reader // Query body (JSON)
}

// DeleteByQueryResponse represents delete by query response.
type DeleteByQueryResponse struct {
	Took             int                      `json:"took"`
	TimedOut         bool                     `json:"timed_out"`
	Total            int                      `json:"total"`
	Deleted          int                      `json:"deleted"`
	Batches          int                      `json:"batches"`
	VersionConflicts int                      `json:"version_conflicts"`
	Failures         []map[string]interface{} `json:"failures"`
}

// CreateIndexRequest represents create index request.
type CreateIndexRequest struct {
	Index string    // Index name
	Body  io.Reader // Mappings and settings (JSON)
}

// IndexExistsRequest represents index exists check request.
type IndexExistsRequest struct {
	Index string // Index name
}

// CountRequest represents count request.
type CountRequest struct {
	Index string    // Index name or pattern
	Body  io.Reader // Query body (JSON), optional
}

// CountResponse represents count response.
type CountResponse struct {
	Count  int                    `json:"count"`
	Shards map[string]interface{} `json:"_shards"`
}

// UpdateByQueryRequest represents update by query request.
type UpdateByQueryRequest struct {
	Index string    // Index name
	Body  io.Reader // Script and query body (JSON)
}

// UpdateByQueryResponse represents update by query response.
type UpdateByQueryResponse struct {
	Took             int                      `json:"took"`
	TimedOut         bool                     `json:"timed_out"`
	Total            int                      `json:"total"`
	Updated          int                      `json:"updated"`
	Batches          int                      `json:"batches"`
	VersionConflicts int                      `json:"version_conflicts"`
	Failures         []map[string]interface{} `json:"failures"`
}
