BUILD_DIR := build
SAN ?= 0
IMAGE ?= asyncmux
DOCKER_PLATFORM ?= linux/amd64
TEST ?= all
TEST_BINS := $(basename $(notdir $(wildcard tests/*.cc)))

ifeq ($(SAN),1)
CMAKE_SAN_FLAG := -DENABLE_SANITIZERS=ON
else
CMAKE_SAN_FLAG := -DENABLE_SANITIZERS=OFF
endif

.PHONY: all configure build run clean rebuild docker docker-build docker-run docker-rebuild

# all: docker-run

configure:
	@echo "Local macOS configure is disabled. Use make docker-build or make docker-run [TEST=...]."
	@false

build:
	docker build --platform $(DOCKER_PLATFORM) -t $(IMAGE) .

run: 
ifeq ($(TEST),all)
	docker run --rm --platform $(DOCKER_PLATFORM) $(IMAGE) /bin/sh -lc 'set -e; for t in $(TEST_BINS); do /app/build/$$t; done'
else
	docker run --rm --platform $(DOCKER_PLATFORM) $(IMAGE) /app/build/$(TEST)
endif

clean:
	rm -rf $(BUILD_DIR)

rebuild: clean build