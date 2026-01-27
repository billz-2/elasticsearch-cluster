package esclient

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDetectIndexTarget(t *testing.T) {
	tests := []struct {
		name      string
		indexName string
		expected  IndexTarget
	}{
		{
			name:      "per-company index with UUID",
			indexName: "products_01234567-89ab-cdef-0123-456789abcdef",
			expected:  IndexTargetPerCompany,
		},
		{
			name:      "shared index",
			indexName: "products_shared",
			expected:  IndexTargetShared,
		},
		{
			name:      "shared index with tier",
			indexName: "products_tier_gold",
			expected:  IndexTargetShared,
		},
		{
			name:      "simple index name",
			indexName: "orders",
			expected:  IndexTargetShared,
		},
		{
			name:      "per-company with prefix",
			indexName: "orders_v2_abcd1234-5678-90ab-cdef-123456789012",
			expected:  IndexTargetPerCompany,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := DetectIndexTarget(tt.indexName)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestQueryMutator_InjectCompanyFilter_PerCompanyIndex(t *testing.T) {
	mutator := NewQueryMutator()

	query := map[string]any{
		"query": map[string]any{
			"match_all": map[string]any{},
		},
	}

	originalJSON, _ := json.Marshal(query)

	err := mutator.InjectCompanyFilter(query, "company-123", IndexTargetPerCompany)
	require.NoError(t, err)

	// Should not modify query for per-company index
	modifiedJSON, _ := json.Marshal(query)
	assert.JSONEq(t, string(originalJSON), string(modifiedJSON))
}

func TestQueryMutator_InjectCompanyFilter_SharedIndex_NoQuery(t *testing.T) {
	mutator := NewQueryMutator()

	query := map[string]any{}

	err := mutator.InjectCompanyFilter(query, "company-456", IndexTargetShared)
	require.NoError(t, err)

	expectedJSON := `{
		"query": {
			"bool": {
				"filter": [
					{"term": {"company_id.keyword": "company-456"}}
				]
			}
		}
	}`

	actualJSON, _ := json.Marshal(query)
	assert.JSONEq(t, expectedJSON, string(actualJSON))
}

func TestQueryMutator_InjectCompanyFilter_SharedIndex_MatchAllQuery(t *testing.T) {
	mutator := NewQueryMutator()

	query := map[string]any{
		"query": map[string]any{
			"match_all": map[string]any{},
		},
	}

	err := mutator.InjectCompanyFilter(query, "company-789", IndexTargetShared)
	require.NoError(t, err)

	expectedJSON := `{
		"query": {
			"bool": {
				"must": [
					{"match_all": {}}
				],
				"filter": [
					{"term": {"company_id.keyword": "company-789"}}
				]
			}
		}
	}`

	actualJSON, _ := json.Marshal(query)
	assert.JSONEq(t, expectedJSON, string(actualJSON))
}

func TestQueryMutator_InjectCompanyFilter_SharedIndex_BoolWithFilter(t *testing.T) {
	mutator := NewQueryMutator()

	query := map[string]any{
		"query": map[string]any{
			"bool": map[string]any{
				"must": []any{
					map[string]any{"term": map[string]any{"status": "active"}},
				},
				"filter": []any{
					map[string]any{"range": map[string]any{"price": map[string]any{"gte": 10}}},
				},
			},
		},
	}

	err := mutator.InjectCompanyFilter(query, "company-abc", IndexTargetShared)
	require.NoError(t, err)

	// Verify company_id filter was appended
	queryMap := query["query"].(map[string]any)
	boolMap := queryMap["bool"].(map[string]any)
	filters := boolMap["filter"].([]any)

	assert.Len(t, filters, 2)

	// Check that company_id filter exists
	found := false
	for _, f := range filters {
		fm := f.(map[string]any)
		if term, ok := fm["term"].(map[string]any); ok {
			if _, hasCompanyID := term["company_id.keyword"]; hasCompanyID {
				assert.Equal(t, "company-abc", term["company_id.keyword"])
				found = true
				break
			}
		}
	}
	assert.True(t, found, "company_id filter not found")
}

func TestQueryMutator_InjectCompanyFilter_SharedIndex_BoolWithFilterObject(t *testing.T) {
	mutator := NewQueryMutator()

	query := map[string]any{
		"query": map[string]any{
			"bool": map[string]any{
				"must": []any{
					map[string]any{"match": map[string]any{"title": "test"}},
				},
				"filter": map[string]any{"term": map[string]any{"is_active": true}},
			},
		},
	}

	err := mutator.InjectCompanyFilter(query, "company-def", IndexTargetShared)
	require.NoError(t, err)

	// Verify filter was converted to array and company_id was added
	queryMap := query["query"].(map[string]any)
	boolMap := queryMap["bool"].(map[string]any)
	filters := boolMap["filter"].([]any)

	assert.Len(t, filters, 2)
}

func TestQueryMutator_InjectCompanyFilter_SharedIndex_BoolWithoutFilter(t *testing.T) {
	mutator := NewQueryMutator()

	query := map[string]any{
		"query": map[string]any{
			"bool": map[string]any{
				"must": []any{
					map[string]any{"match": map[string]any{"name": "product"}},
				},
			},
		},
	}

	err := mutator.InjectCompanyFilter(query, "company-ghi", IndexTargetShared)
	require.NoError(t, err)

	expectedJSON := `{
		"query": {
			"bool": {
				"must": [
					{"match": {"name": "product"}}
				],
				"filter": [
					{"term": {"company_id.keyword": "company-ghi"}}
				]
			}
		}
	}`

	actualJSON, _ := json.Marshal(query)
	assert.JSONEq(t, expectedJSON, string(actualJSON))
}

func TestQueryMutator_InjectCompanyFilter_SharedIndex_MissingCompanyID(t *testing.T) {
	mutator := NewQueryMutator()

	query := map[string]any{
		"query": map[string]any{
			"match_all": map[string]any{},
		},
	}

	err := mutator.InjectCompanyFilter(query, "", IndexTargetShared)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "companyID required")
}

func TestQueryMutator_InjectCompanyFilter_SharedIndex_ComplexQuery(t *testing.T) {
	mutator := NewQueryMutator()

	query := map[string]any{
		"query": map[string]any{
			"bool": map[string]any{
				"must": []any{
					map[string]any{"match": map[string]any{"title": "laptop"}},
				},
				"should": []any{
					map[string]any{"term": map[string]any{"brand": "apple"}},
					map[string]any{"term": map[string]any{"brand": "dell"}},
				},
				"filter": []any{
					map[string]any{"range": map[string]any{"price": map[string]any{"gte": 500}}},
				},
				"minimum_should_match": 1,
			},
		},
		"sort": []any{
			map[string]any{"price": map[string]any{"order": "asc"}},
		},
	}

	err := mutator.InjectCompanyFilter(query, "company-jkl", IndexTargetShared)
	require.NoError(t, err)

	// Verify structure preserved
	queryMap := query["query"].(map[string]any)
	boolMap := queryMap["bool"].(map[string]any)

	// Check must clause preserved
	must := boolMap["must"].([]any)
	assert.Len(t, must, 1)

	// Check should clause preserved
	should := boolMap["should"].([]any)
	assert.Len(t, should, 2)

	// Check filter has both original and company_id
	filters := boolMap["filter"].([]any)
	assert.Len(t, filters, 2)

	// Check minimum_should_match preserved
	assert.Equal(t, 1, boolMap["minimum_should_match"])

	// Check sort preserved
	sort := query["sort"].([]any)
	assert.Len(t, sort, 1)
}
