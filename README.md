# File segment merger

This project validates versioned file manifests, loads raw or gzip-compressed segments, verifies logical content, merges concurrent reads in manifest order, preserves sparse offsets, supports version-bound resume checkpoints, commits staged output, and records the final artifact digest in an index.

Run `go test ./...` or `go run ./cmd/merge-demo`.
