FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    clang-14 \
    lld \
    libc++-14-dev \
    libc++abi-14-dev \
    ninja-build \
    git \
    curl \
    zip \
    unzip \
    tar \
    python3 \
    pkg-config \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . /app

RUN rm -rf build

RUN cmake -S /app -B /app/build -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_C_COMPILER=clang-14 \
    -DCMAKE_CXX_COMPILER=clang++-14 \
    -DCMAKE_CXX_FLAGS="-stdlib=libc++" \
    -DCMAKE_EXE_LINKER_FLAGS="-stdlib=libc++"

RUN cmake --build /app/build -j

CMD ["/app/build/asyncmux_tests"]