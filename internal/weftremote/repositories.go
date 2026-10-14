package weftremote

import (
	"net/http"

	"shuttle/internal/ledger"
)

// handleRepositories lists every repository with its holder and dispatch
// state (§6.2, §7.5), the projects bound to each as tk reads them now.
func (s *Service) handleRepositories(w http.ResponseWriter) {
	if _, err := s.syncCatalog(); err != nil {
		s.catalogError(w, err)
		return
	}
	s.stateMu.RLock()
	repositories, err := s.store.WeftRepositories()
	var asOf int64
	if err == nil {
		asOf, err = s.store.WeftAsOf()
	}
	s.stateMu.RUnlock()
	if err != nil {
		s.v2Internal(w, err)
		return
	}
	listed := make([]map[string]any, 0, len(repositories))
	for _, r := range repositories {
		listed = append(listed, r.Resource())
	}
	s.read(w, asOf, map[string]any{"repositories": listed})
}

// handleContinue lifts a landing pause (§7.5); the entries it held dispatch
// in the same transaction.
func (s *Service) handleContinue(w http.ResponseWriter, r *http.Request, repositoryID string) {
	s.mutateV2(w, r, func(body []byte, _ string) (v2Commit, error) {
		var req revisionRequest
		if err := decodeV2(body, &req, "revision"); err != nil {
			return refusedBy(err), nil
		}
		if !validProtocolID(repositoryID) {
			return refused("repository_not_found", "%s is not a repository on host %s", repositoryID, s.hostID), nil
		}
		c, err := s.readCatalog()
		if err != nil {
			return nil, err
		}
		return func(tx *ledger.WeftTx) (v2Outcome, error) {
			if _, err := tx.ContinueRepository(repositoryID, req.Revision); err != nil {
				return v2Outcome{}, err
			}
			if err := tx.Dispatch(s.dispatchConfig(c)); err != nil {
				return v2Outcome{}, err
			}
			repository, err := tx.Repository(repositoryID)
			if err != nil {
				return v2Outcome{}, err
			}
			return v2Outcome{status: http.StatusOK, fields: map[string]any{"repository": repository.Resource()}}, nil
		}, nil
	})
}
