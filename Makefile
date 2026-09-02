.PHONY: dev install

# dev builds the shuttle binary locally for running unreleased changes.
dev:
	go build -o ./shuttle ./cmd/shuttle

# install puts the binary where the launchd daemon resolves it (design §13).
install:
	mkdir -p $(HOME)/.local/bin
	go build -o $(HOME)/.local/bin/shuttle ./cmd/shuttle
