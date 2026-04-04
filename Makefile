BUILD_DIR := build
SAN ?= 0
IMAGE ?= asyncmux

ifeq ($(SAN),1)
CMAKE_SAN_FLAG := -DENABLE_SANITIZERS=ON
else
CMAKE_SAN_FLAG := -DENABLE_SANITIZERS=OFF
endif

.PHONY: all configure build run clean rebuild docker docker-build docker-run docker-rebuild

all: build

configure:
	cmake -S . -B $(BUILD_DIR) -DCMAKE_BUILD_TYPE=Debug $(CMAKE_SAN_FLAG)

build: configure
	cmake --build $(BUILD_DIR)

run: build
	./$(BUILD_DIR)/asyncmux_tests

clean:
	rm -rf $(BUILD_DIR)

rebuild: clean build

docker: docker-build

docker-build:
	docker build --platform linux/amd64 -t $(IMAGE) .

docker-run:
	docker run --rm $(IMAGE)

docker-rebuild: docker-build docker-run
