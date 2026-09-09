package observer

import "strings"

// ContractVersion is the interchange version this binary writes. Every payload
// carries it because the notebook outlives the binary: an observation read two
// years from now has to say which rules it was recorded under rather than be
// guessed at from its shape.
const ContractVersion = "observer/v1"

// supportedVersions is what this binary decodes. A version is added here only
// with the code that reads it; historical observations are never rewritten
// into a newer one, so the set only ever grows.
var supportedVersions = []string{ContractVersion}

// Envelope is the version field every payload carries. It is embedded rather
// than defaulted: a payload that omits it is refused, since a missing version
// silently read as the current one is exactly the mistake versioning exists to
// catch.
type Envelope struct {
	ContractVersion string `json:"contract_version"`
}

// checkVersion names both the field and every version this binary supports; a
// producer that sent the wrong one needs to know what it may send instead.
func (e Envelope) checkVersion() error {
	for _, v := range supportedVersions {
		if e.ContractVersion == v {
			return nil
		}
	}
	if e.ContractVersion == "" {
		return fieldErr("contract_version", "required, one of %s", strings.Join(supportedVersions, ", "))
	}
	return fieldErr("contract_version", "%q is not supported; this binary reads %s", e.ContractVersion, strings.Join(supportedVersions, ", "))
}
