#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_DIR="${ROOT_DIR}/data_analysis/raw"

ASYNC_BIN="${ROOT_DIR}/../build/perf_async"
BLOCK_BIN="${ROOT_DIR}/../build/perf_block"

OPS=10000
THREADS=8
CONCURRENCY=16
SEED=1

mkdir -p "${OUT_DIR}"

if [[ ! -x "${ASYNC_BIN}" ]]; then
  echo "error: missing async benchmark binary at ${ASYNC_BIN}" >&2
  exit 1
fi

if [[ ! -x "${BLOCK_BIN}" ]]; then
  echo "error: missing blocking benchmark binary at ${BLOCK_BIN}" >&2
  exit 1
fi

echo "== Running summary benchmarks =="

"${ASYNC_BIN}" \
  --ops "${OPS}" \
  --concurrency "${CONCURRENCY}" \
  --threads "${THREADS}" \
  --seed "${SEED}" \
  --csv \
  > "${OUT_DIR}/async_summary.csv"

"${BLOCK_BIN}" \
  --ops "${OPS}" \
  --concurrency "${CONCURRENCY}" \
  --threads "${THREADS}" \
  --seed "${SEED}" \
  --csv \
  > "${OUT_DIR}/block_summary.csv"

echo "== Running fanout concurrency sweep =="

"${ASYNC_BIN}" \
  --ops "${OPS}" \
  --threads "${THREADS}" \
  --seed "${SEED}" \
  --workload fanout_read_multitier \
  --sweep-concurrency \
  --csv \
  > "${OUT_DIR}/async_fanout_concurrency.csv"

"${BLOCK_BIN}" \
  --ops "${OPS}" \
  --threads "${THREADS}" \
  --seed "${SEED}" \
  --workload fanout_read_multitier \
  --sweep-concurrency \
  --csv \
  > "${OUT_DIR}/block_fanout_concurrency.csv"

echo "== Running foreground multitier concurrency sweep =="

"${ASYNC_BIN}" \
  --ops "${OPS}" \
  --threads "${THREADS}" \
  --seed "${SEED}" \
  --workload foreground_rw_multitier \
  --sweep-concurrency \
  --csv \
  > "${OUT_DIR}/async_foreground_rw_multitier_concurrency.csv"

"${BLOCK_BIN}" \
  --ops "${OPS}" \
  --threads "${THREADS}" \
  --seed "${SEED}" \
  --workload foreground_rw_multitier \
  --sweep-concurrency \
  --csv \
  > "${OUT_DIR}/block_foreground_rw_multitier_concurrency.csv"

echo "== Done =="
echo "Wrote results to:"
echo "  ${OUT_DIR}/async_summary.csv"
echo "  ${OUT_DIR}/block_summary.csv"
echo "  ${OUT_DIR}/async_fanout_concurrency.csv"
echo "  ${OUT_DIR}/block_fanout_concurrency.csv"
echo "  ${OUT_DIR}/async_foreground_rw_multitier_concurrency.csv"
echo "  ${OUT_DIR}/block_foreground_rw_multitier_concurrency.csv"