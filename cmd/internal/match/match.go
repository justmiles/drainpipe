// Package match provides simple wildcard pattern matching for table names.
package match

import (
	"path"
	"sort"
	"strings"
)

// Tables filters a list of table names against a set of patterns.
// Patterns support wildcards (*, ?) and exact matches.
func Tables(allTables []string, patterns []string) []string {
	if len(patterns) == 0 {
		return allTables
	}

	seen := map[string]bool{}
	var matched []string

	for _, pattern := range patterns {
		for _, table := range allTables {
			if seen[table] {
				continue
			}
			// Try exact match first
			if pattern == table {
				matched = append(matched, table)
				seen[table] = true
				continue
			}
			// Then try glob match
			ok, err := path.Match(pattern, table)
			if err == nil && ok {
				matched = append(matched, table)
				seen[table] = true
			}
		}
	}

	return matched
}

// Suggest returns up to maxResults table names that are similar to the given
// patterns. Used for "did you mean?" suggestions.
func Suggest(allTables []string, patterns []string, maxResults int) []string {
	type scored struct {
		name  string
		score int
	}

	var candidates []scored

	for _, pattern := range patterns {
		// Strip wildcard chars for substring matching
		clean := strings.NewReplacer("*", "", "?", "").Replace(pattern)
		if clean == "" {
			continue
		}

		for _, table := range allTables {
			if strings.Contains(table, clean) {
				candidates = append(candidates, scored{table, len(clean)})
			}
		}
	}

	// Sort by match quality (longer substring match = better)
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].score > candidates[j].score
	})

	// Deduplicate and limit
	seen := map[string]bool{}
	var results []string
	for _, c := range candidates {
		if seen[c.name] {
			continue
		}
		seen[c.name] = true
		results = append(results, c.name)
		if len(results) >= maxResults {
			break
		}
	}

	return results
}
