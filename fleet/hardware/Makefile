.PHONY: localTest testSchemas testAgainstTargetTag testLatestAgainstPreviousTag testCommitAgainstLatestTag testSchemaParsable

BRANCH ?= ""
TARGET_TAG ?= ""
LATEST_TAG ?= ""
TARGET_MAJOR_VERSION := $$(echo ${TARGET_TAG} | cut -d'-' -f 2 | cut -d'.' -f 1)
CURRENT_MAJOR_VERSION := $$(echo ${LATEST_TAG} | cut -d'-' -f 2 | cut -d'.' -f 1)


localTest:
	rm -rf master-schemas
	rm -rf src/test/resources
	git fetch
	git checkout master
	mkdir master-schemas
	cp ./schemas/avro/*.* ./master-schemas
	git checkout $(BRANCH)
	mkdir src/test/resources
	mkdir src/test/resources/current
	mkdir src/test/resources/master
	cp ./schemas/avro/*.* ./src/test/resources/current
	cp ./master-schemas/*.* ./src/test/resources/master
	sbt clean test
	rm -rf master-schemas
	rm -rf src/test/resources

testSchemas:
	git checkout $(TARGET_TAG)
	mkdir master-schemas
	cp ./schemas/avro/*.* ./master-schemas
	git checkout $(CI_COMMIT_REF_NAME)
	mkdir src/test/resources
	mkdir src/test/resources/current
	mkdir src/test/resources/master
	cp ./schemas/avro/*.* ./src/test/resources/current
	cp ./master-schemas/*.* ./src/test/resources/master
	sbt clean test

testSchemaParsable:
	mkdir src/test/resources
	mkdir src/test/resources/current
	mkdir src/test/resources/master
	cp ./schemas/avro/*.* ./src/test/resources/current
	cp ./schemas/avro/*.* ./src/test/resources/master
	sbt clean test

testAgainstTargetTag:
	@echo "Latest tag: $(LATEST_TAG). Target tag: $(TARGET_TAG)."
	@if [ "$(TARGET_MAJOR_VERSION)" = "$(CURRENT_MAJOR_VERSION)" ]; then \
		make testSchemas; \
	else \
		echo -e "\e[32m::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::"; \
		echo -e "\e[32m DIFFERENT MAJOR VERSIONS! CURRENT $(CURRENT_MAJOR_VERSION). TARGET $(TARGET_MAJOR_VERSION). "; \
		echo -e "\e[32m TESTING BASIC SCHEMA VALIDITY CHECKING COMPATIBILITY AGAINST THEMSELVES! "; \
		echo -e "\e[32m::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::"; \
		echo -e "\e[39m "; \
		make testSchemaParsable; \
	fi


testLatestAgainstPreviousTag:
	apt-get install git
	git checkout tags/$(CI_COMMIT_REF_NAME)
	git fetch
	git tag -l | xargs git tag -d
	git fetch --tags
	make testAgainstTargetTag \
	LATEST_TAG=$(shell git describe --abbrev=0 --tags `git rev-list --tags --max-count=1`) \
	TARGET_TAG=$(shell git describe --abbrev=0 --tags `git rev-list --tags --skip=1 --max-count=1`) \
	BRANCH=$(shell git rev-parse --abbrev-ref HEAD)

testCommitAgainstLatestTag:
	apt-get install git
	git checkout $(CI_COMMIT_REF_NAME)
	git pull
	git tag -l | xargs git tag -d
	git fetch --tags
	@echo -e "\e[32m:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::"
	@echo -e "\e[32m:: NO TAG FOUND; TESTING COMPATIBILITY AGAINST LATEST TAG! ::"
	@echo -e "\e[32m:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::"
	@echo -e "\e[39m "
	make testAgainstTargetTag \
	LATEST_TAG=$(shell git describe --abbrev=0 --tags `git rev-list --tags --max-count=1`) \
	TARGET_TAG=$(shell git describe --abbrev=0 --tags `git rev-list --tags --max-count=1`) \
	BRANCH=$(shell git rev-parse --abbrev-ref HEAD)