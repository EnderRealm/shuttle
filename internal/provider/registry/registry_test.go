package registry

import (
	"slices"
	"testing"

	"shuttle/internal/agentdef"
)

// A provider agentdef accepts but the registry cannot run would fail the run at
// step 3 rather than at load; one the registry runs but agentdef refuses is
// unreachable. The two sets are the same set.
func TestTheRegistryMatchesTheManifestsAllowedProviders(t *testing.T) {
	names := make([]string, 0, len(Providers))
	for name := range Providers {
		names = append(names, name)
	}
	slices.Sort(names)
	allowed := slices.Clone(agentdef.AllowedProviders)
	slices.Sort(allowed)
	if !slices.Equal(names, allowed) {
		t.Fatalf("registered = %v, allowed = %v", names, allowed)
	}
}
