
DOCKER_IP      ?= 127.0.0.1
AWS_ENDPOINT   ?= $(DOCKER_IP):4100
COVERAGE_FILE   = coverage.out
COVERAGE_FILES  = coverage/*.cov

-include .env
export

.PHONY: env
env:
	@printenv

.PHONY: up
up:
	@mkdir -p .data/pulsar
	@docker-compose up -d
	@./scripts/wait-for-sqs.sh

.PHONY: down
down:
	@docker-compose down

.PHONY: test
test: up integration

integration:
	@mkdir -p coverage
	@go test -race -v -tags=integration -coverpkg=./... -coverprofile=./coverage/base.cov ./...
	@cd aws && go test -race -v -tags=integration -coverpkg=./... -coverprofile=../coverage/aws.cov ./...

.PHONY: clean
clean:
	@rm -rf $(COVERAGE_FILES)
	@rm -rf $(COVERAGE_FILE)

.PHONY: coverage
coverage: clean test coverage-merge
	@go tool cover -func=$(COVERAGE_FILE)

.PHONY: coverage-html
coverage-html: clean test coverage-merge
	@go tool cover -html=$(COVERAGE_FILE)

.PHONY: coverage-merge
coverage-merge:
	@echo 'mode: atomic' > $(COVERAGE_FILE)
	@tail -q -n +2 $(COVERAGE_FILES) >> $(COVERAGE_FILE)
	@sed -i'.original' "/internal/d" coverage.out $(COVERAGE_FILE)

.PHONY: proto
proto:
	@protoc -I=./internal/proto --go_opt=paths=source_relative --go_out=./internal/proto test.proto
