package esclient

import (
	"strings"

	"github.com/pkg/errors"
)

// DetectIndexTarget determines if index is per-company or shared
func DetectIndexTarget(indexName string) IndexTarget {
	parts := strings.Split(indexName, "_")
	if len(parts) < 2 {
		return IndexTargetShared
	}

	lastPart := parts[len(parts)-1]

	// UUID pattern: 36 chars with 4 dashes
	if len(lastPart) == 36 && strings.Count(lastPart, "-") == 4 {
		return IndexTargetPerCompany
	}

	return IndexTargetShared
}

type QueryMutator struct{}

func NewQueryMutator() *QueryMutator {
	return &QueryMutator{}
}

// InjectCompanyFilter adds company_id term filter for shared indices
func (m *QueryMutator) InjectCompanyFilter(query map[string]any, companyID string, target IndexTarget) error {
	if target == IndexTargetPerCompany {
		return nil
	}

	if companyID == "" {
		return errors.New("companyID required for shared index")
	}

	companyFilter := map[string]any{
		"term": map[string]any{
			"company_id.keyword": companyID,
		},
	}

	queryMap, hasQuery := query["query"].(map[string]any)
	if !hasQuery {
		query["query"] = map[string]any{
			"bool": map[string]any{
				"filter": []any{companyFilter},
			},
		}
		return nil
	}

	boolMap, hasBool := queryMap["bool"].(map[string]any)
	if hasBool {
		return m.injectIntoBool(boolMap, companyFilter)
	}

	// Wrap non-bool query
	originalQuery := query["query"]
	query["query"] = map[string]any{
		"bool": map[string]any{
			"must":   []any{originalQuery},
			"filter": []any{companyFilter},
		},
	}
	return nil
}

func (m *QueryMutator) injectIntoBool(boolMap map[string]any, filter map[string]any) error {
	filterVal, hasFilter := boolMap["filter"]

	if !hasFilter {
		boolMap["filter"] = []any{filter}
		return nil
	}

	switch f := filterVal.(type) {
	case []any:
		boolMap["filter"] = append(f, filter)
	case map[string]any:
		boolMap["filter"] = []any{f, filter}
	default:
		return errors.Errorf("unexpected filter type: %T", filterVal)
	}

	return nil
}
