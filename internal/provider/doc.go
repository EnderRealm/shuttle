// Package provider holds the input providers, one package each beneath it,
// and the pieces they share: the window resolved from the run's slot, the §8
// envelope every document carries and the failure the runner reports them
// with. A provider writes exactly one enveloped JSON document under the run's
// inputs/.
package provider
