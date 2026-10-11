//go:build !darwin

package sandbox

// RepositoryIdentity falls back to the resolved path off macOS. Shuttle's
// installed service is macOS-only; this keeps fixture builds portable.
func RepositoryIdentity(repo string) (string, error) {
	return CanonicalRepository(repo)
}

// filesystemSpelling is the resolved path unchanged off macOS, where the
// volumes Shuttle meets fold no case.
func filesystemSpelling(path string) (string, error) {
	return path, nil
}
