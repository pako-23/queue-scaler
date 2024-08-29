PROGS := approximator autoscaler

.PHONY: all
all: $(PROGS)

approximator: $(shell find internal/ -name *.go) $(shell find cmd/approximator/ -name *.go)
	go build -tags netgo -ldflags='-s -w' -o $@ ./cmd/$@

autoscaler: $(shell find internal/ -name *.go) $(shell find cmd/autoscaler/ -name *.go)
	go build -tags netgo -ldflags='-s -w' -o $@ ./cmd/$@

.PHONY: clean
clean:
	- go clean -cache -testcache
	- rm -rf $(PROGS)

.PHONY: check
check:
	go test -covermode=atomic -race -coverprofile=coverage.cov ./...
	go tool cover -html=coverage.cov -o coverage.html

.PHONY: tidy
tidy:
	go mod tidy

.PHONY: format
format:
	go fmt ./...
