
export DOCKER_IP    ?= 127.0.0.1
export AWS_ENDPOINT ?= $(DOCKER_IP):4100

COVERAGE_FILE  = coverage.out
COVERAGE_FILES = coverage/*.cov

.PHONY: up
up:
	@mkdir -p .data/pulsar
	@docker-compose up -d
	@./scripts/wait-for-sqs.sh

.PHONY: down
down:
	@docker-compose down

.PHONY: test
test: up
	@mkdir -p coverage
	@go test -race -v -tags=integration -coverprofile=./coverage/integration.cov ./...

.PHONY: clean
clean:
	@rm -rf $(COVERAGE_FILE)

.PHONY: coverage
coverage: test coverage-merge
	@go tool cover -func=$(COVERAGE_FILE)

.PHONY: coverage-html
coverage-html: test coverage-merge
	@go tool cover -html=$(COVERAGE_FILE)

.PHONY: coverage-merge
coverage-merge:
	@echo 'mode: atomic' > $(COVERAGE_FILE)
	@tail -q -n +2 $(COVERAGE_FILES) >> $(COVERAGE_FILE)
