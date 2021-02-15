
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
	@cd aws && go test -race -v -tags=integration -coverpkg=./... -coverprofile=../coverage/awsv2.cov ./...

.PHONY: clean
clean:
	@rm -rf $(COVERAGE_FILES)
	@rm -rf coverage.*

.PHONY: coverage
coverage: clean test coverage-merge
	@cd aws && go tool cover -func=../$(COVERAGE_FILE)

.PHONY: coverage-html
coverage-html: clean test coverage-merge
	@cd aws && go tool cover -html=../$(COVERAGE_FILE)

.PHONY: coverage-merge
coverage-merge:
	@echo 'mode: atomic' > $(COVERAGE_FILE)
	@tail -q -n +2 $(COVERAGE_FILES) >> $(COVERAGE_FILE)
	@sed -i'.original' "/internal/d" coverage.out $(COVERAGE_FILE)
