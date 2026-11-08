//go:build !darwin

package sandbox

// filesystemSpelling is the resolved path unchanged off macOS, where the
// volumes Shuttle meets fold no case.
func filesystemSpelling(path string) (string, error) {
	return path, nil
}
