package weftremote

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"shuttle/internal/keychain"
)

const (
	imageReaderCapability = "image_reader"
	// imageReaderUnconfigured is service-image-reader-not-served.json's reason.
	imageReaderUnconfigured = "no image-reader token is configured on this host"
	// imageReaderReadBound bounds the Keychain read discovery waits on. It is
	// well under the updater's bound on reading /v2/service, so a Keychain that
	// does not answer costs pictures and not an update.
	imageReaderReadBound = 2 * time.Second
)

// resolvedCapabilities is capabilities with image_reader as the Keychain
// answers now, and the token when it is served. Each call reads the Keychain,
// so only discovery and the image-reader route make it, holding no lock.
func (s *Service) resolvedCapabilities() ([]capability, string) {
	listed := s.capabilities()
	for i := range listed {
		if listed[i].ID != imageReaderCapability || listed[i].Reason != "" {
			continue
		}
		token, err := s.imageReaderToken()
		if err != nil {
			listed[i].Reason = err.Error()
		}
		return listed, token
	}
	return listed, ""
}

// imageReaderToken reads Folio's image-reader token at use time, so a rotation
// needs no restart (§5.6). It reads what security -g prints, so a value holding
// a byte outside visible ASCII is judged by its bytes and not by their hex. The
// error, which discovery serves, names the item and never the value.
func (s *Service) imageReaderToken() (string, error) {
	item := s.cfg.Weft.ImageReader.KeychainItem
	read := s.printPassword
	if read == nil {
		read = func(item string) ([]byte, error) {
			ctx, cancel := context.WithTimeout(context.Background(), imageReaderReadBound)
			defer cancel()
			return keychain.PrintedPassword(ctx, item)
		}
	}
	printed, err := read(item)
	var token string
	if err == nil {
		token, err = keychain.ParsePassword(item, printed)
	}
	if err != nil {
		return "", fmt.Errorf("weft.image_reader.keychain_item: %w", err)
	}
	if !visibleASCII(token) {
		return "", fmt.Errorf("weft.image_reader.keychain_item: keychain item %q: not a token of visible ASCII characters", item)
	}
	return token, nil
}

// visibleASCII reports whether value is one or more visible ASCII characters,
// safe as a header value as it stands.
func visibleASCII(value string) bool {
	for i := range len(value) {
		if value[i] < '!' || value[i] > '~' {
			return false
		}
	}
	return value != ""
}

// handleImageReader hands a client Folio's origin and its image-reader token
// (§5.6), the one response that carries it.
func (s *Service) handleImageReader(w http.ResponseWriter) {
	listed, token := s.resolvedCapabilities()
	if token == "" {
		s.capabilityNotServed(w, listed, imageReaderCapability)
		return
	}
	s.stateMu.RLock()
	asOf, err := s.store.WeftAsOf()
	s.stateMu.RUnlock()
	if err != nil {
		s.v2Internal(w, err)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	s.read(w, asOf, map[string]any{imageReaderCapability: map[string]any{
		"origin": s.cfg.Weft.ImageReader.Origin, "token": token,
	}})
}
