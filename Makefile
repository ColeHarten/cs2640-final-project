BUILD_DIR := build
SAN ?= 0
TEST ?= all
CXX ?= /usr/bin/clang++-12

TEST_BINS := $(basename $(notdir $(wildcard tests/*.cc)))

ifeq ($(SAN),1)
CMAKE_SAN_FLAG := -DENABLE_SANITIZERS=ON
else
CMAKE_SAN_FLAG := -DENABLE_SANITIZERS=OFF
endif

CMAKE_FLAGS := \
	-DCMAKE_BUILD_TYPE=RelWithDebInfo \
	$(CMAKE_SAN_FLAG) \
	-DCMAKE_CXX_COMPILER=$(CXX)

.PHONY: all configure build run clean rebuild

all: build

configure:
	rm -rf $(BUILD_DIR)
	cmake -S . -B $(BUILD_DIR) $(CMAKE_FLAGS)

build: configure
	cmake --build $(BUILD_DIR) --parallel

run: build
ifeq ($(TEST),all)
	@status=0; \
	for t in $(TEST_BINS); do \
		echo "Running $$t"; \
		./$(BUILD_DIR)/$$t || { status=$$?; break; }; \
	done; \
	if [ $$status -eq 0 ]; then \
		printf "\033[1;32mAll tests passed.\033[0m\n"; \
	fi; \
	exit $$status
else
	./$(BUILD_DIR)/$(TEST)
endif

clean:
	rm -rf $(BUILD_DIR)

rebuild: clean build

rebuild: clean build

docker-build:
	docker build --platform $(DOCKER_PLATFORM) -t $(IMAGE) .

docker-run:
ifeq ($(TEST),all)
	docker run --rm --platform $(DOCKER_PLATFORM) --privileged $(IMAGE) /bin/sh -lc 'status=0; for t in $(TEST_BINS); do /app/build/$$t || { status=$$?; break; }; done; if [ $$status -eq 0 ]; then printf "\033[1;32mAll tests passed.\033[0m\n"; fi; exit $$status'
else
	docker run --rm --platform $(DOCKER_PLATFORM) --privileged $(IMAGE) /app/build/$(TEST)
endif

docker: docker-run

docker-rebuild: docker-build