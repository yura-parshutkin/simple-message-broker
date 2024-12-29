build: fmt lint unit coverage
	go build -o ./application ./
.PHONY: run

prepare:
	go mod download -x
	go generate $(shell go list ./... | grep -v ./.go/)
	go mod tidy
.PHONY: prepare

fmt:
	go fmt $(shell go list -e -f '{{.Dir}}' ./... | grep -v ./.go/)
.PHONY: fmt

unit: prepare
	go test -short -coverprofile coverage.out.tmp $(shell go list -e -f '{{.Dir}}' ./... | grep -v ./.go/)
	cat coverage.out.tmp | grep -v ".gen.go" > coverage.out
	go tool cover -func coverage.out
.PHONY: unit

coverage:
	go test -short -race -coverprofile coverage.out.tmp ./...
	cat coverage.out.tmp | grep -v ".gen.go" > coverage.out
.PHONY: coverage

lint: prepare
	golangci-lint cache clean
	golangci-lint run
.PHONY: lint