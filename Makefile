
.PHONY: clean
clean:
	go clean -cache -testcache

.PHONY: check
check:
	go test -covermode=atomic -race -coverprofile=coverage.cov ./...
	go tool cover -html=coverage.cov -o coverage.html

format:
	go fmt ./...
