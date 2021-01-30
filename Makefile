
export DOCKER_IP ?= 127.0.0.1

COVERAGE_FILE  = coverage.out
COVERAGE_FILES = coverage/*.cov

.PHONY: up
up:
	@docker-compose up -d
	@./scripts/wait-for-sqs.sh

.PHONY: test
test: up
	@go test -race -v -tags=integration -coverpkg=./... -coverprofile=coverage/pubsub.cov ./...
	@cd aws
	@go test -race -v -tags=integration -coverpkg=./... -coverprofile=coverage/aws.cov ./...

.PHONY: clean
clean:
	rm -rf $(COVERAGE_FILE)

.PHONY: coverage
coverage: test
	@go tool cover -func=$(COVERAGE_FILE)

.PHONY: coverage-html
coverage-html: test
	@go tool cover -html=$(COVERAGE_FILE)

.PHONY: coverage-merge
coverage-merge:
	@echo 'mode: atomic' > $(COVERAGE_FILE)
	@tail -q -n +2 $(COVERAGE_FILES) >> $(COVERAGE_FILE)
