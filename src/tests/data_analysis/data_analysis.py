import csv
import os

import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np


RAW_DIR = "raw"
OUTDIR = "figs"

SUMMARY_ASYNC = os.path.join(RAW_DIR, "async_summary.csv")
SUMMARY_BLOCK = os.path.join(RAW_DIR, "block_summary.csv")

FANOUT_ASYNC = os.path.join(RAW_DIR, "async_fanout_concurrency.csv")
FANOUT_BLOCK = os.path.join(RAW_DIR, "block_fanout_concurrency.csv")

FGRW_ASYNC = os.path.join(RAW_DIR, "async_foreground_rw_multitier_concurrency.csv")
FGRW_BLOCK = os.path.join(RAW_DIR, "block_foreground_rw_multitier_concurrency.csv")

WORKLOAD_ORDER = [
    "sequential_write",
    "sequential_read",
    "random_read",
    "random_write",
    "mixed_80r_20w",
    "fanout_read_multitier",
    "foreground_rw_multitier",
]

PRETTY_LABELS = {
    "sequential_write": "Seq Write",
    "sequential_read": "Seq Read",
    "random_read": "Rand Read",
    "random_write": "Rand Write",
    "mixed_80r_20w": "Mixed 80/20",
    "fanout_read_multitier": "Fanout Read",
    "foreground_rw_multitier": "Multitier RW",
}


def ensure_outdir() -> None:
    os.makedirs(OUTDIR, exist_ok=True)


def outpath(filename: str) -> str:
    return os.path.join(OUTDIR, filename)


def read_csv_rows(path: str) -> list[dict]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Missing CSV file: {path}")

    with open(path, "r", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        raise ValueError(f"CSV file is empty: {path}")

    return rows


def parse_numeric_fields(rows: list[dict]) -> list[dict]:
    int_fields = {"ops", "total_bytes", "concurrency"}
    float_fields = {
        "seconds",
        "ops_per_sec",
        "mib_per_sec",
        "avg_us",
        "p50_us",
        "p95_us",
        "p99_us",
        "max_us",
    }

    parsed = []
    for row in rows:
        out = dict(row)
        for key in int_fields:
            if key in out:
                out[key] = int(float(out[key]))
        for key in float_fields:
            if key in out:
                out[key] = float(out[key])
        parsed.append(out)
    return parsed


def load_rows(path: str) -> list[dict]:
    return parse_numeric_fields(read_csv_rows(path))


def summary_to_metric_arrays(rows: list[dict], workload_order: list[str]) -> dict[str, np.ndarray]:
    by_workload = {row["workload"]: row for row in rows}

    missing = [w for w in workload_order if w not in by_workload]
    if missing:
        raise ValueError(f"Missing workloads in summary CSV: {missing}")

    metrics = {}
    metric_keys = [
        "ops_per_sec",
        "mib_per_sec",
        "avg_us",
        "p50_us",
        "p95_us",
        "p99_us",
        "max_us",
    ]

    for key in metric_keys:
        metrics[key] = np.array([by_workload[w][key] for w in workload_order], dtype=float)

    return metrics


def sweep_to_xy(rows: list[dict], expected_workload: str, y_key: str) -> tuple[np.ndarray, np.ndarray]:
    filtered = [r for r in rows if r["workload"] == expected_workload]
    if not filtered:
        raise ValueError(f"No rows found for workload '{expected_workload}'")

    filtered.sort(key=lambda r: r["concurrency"])
    x = np.array([r["concurrency"] for r in filtered], dtype=int)
    y = np.array([r[y_key] for r in filtered], dtype=float)
    return x, y


def grouped_bar_chart(labels, a_vals, b_vals, ylabel, title, filename, logy=False):
    x = np.arange(len(labels))
    width = 0.38

    fig, ax = plt.subplots(figsize=(10, 4.8))
    ax.bar(x - width / 2, a_vals, width, label="AsyncMux")
    ax.bar(x + width / 2, b_vals, width, label="BlockingMux")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=25, ha="right")
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    if logy:
        ax.set_yscale("log")
    ax.legend()

    fig.tight_layout()
    fig.savefig(outpath(filename), dpi=300, bbox_inches="tight")
    plt.close(fig)


def grouped_bar_chart_single_series(labels, vals, ylabel, title, filename):
    x = np.arange(len(labels))

    fig, ax = plt.subplots(figsize=(10, 4.8))
    ax.bar(x, vals)
    ax.axhline(1.0, linestyle="--", linewidth=1)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=25, ha="right")
    ax.set_ylabel(ylabel)
    ax.set_title(title)

    fig.tight_layout()
    fig.savefig(outpath(filename), dpi=300, bbox_inches="tight")
    plt.close(fig)


def latency_triptych(labels, a_p50, a_p95, a_p99, b_p50, b_p95, b_p99, filename):
    x = np.arange(len(labels))
    width = 0.13

    fig, ax = plt.subplots(figsize=(12, 5.2))
    ax.bar(x - 2.5 * width, a_p50, width, label="Async p50")
    ax.bar(x - 1.5 * width, a_p95, width, label="Async p95")
    ax.bar(x - 0.5 * width, a_p99, width, label="Async p99")
    ax.bar(x + 0.5 * width, b_p50, width, label="Blocking p50")
    ax.bar(x + 1.5 * width, b_p95, width, label="Blocking p95")
    ax.bar(x + 2.5 * width, b_p99, width, label="Blocking p99")

    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=25, ha="right")
    ax.set_ylabel("Latency (us)")
    ax.set_yscale("log")
    ax.set_title("Latency distribution summary by workload")
    ax.legend(ncol=3)

    fig.tight_layout()
    fig.savefig(outpath(filename), dpi=300, bbox_inches="tight")
    plt.close(fig)


def line_chart_two_series(x1, y1, x2, y2, ylabel, title, filename, label1="AsyncMux", label2="BlockingMux", logy=False):
    fig, ax = plt.subplots(figsize=(8.5, 4.8))
    ax.plot(x1, y1, marker="o", label=label1)
    ax.plot(x2, y2, marker="o", label=label2)
    ax.set_xlabel("Concurrency")
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.set_xticks(sorted(set(x1.tolist()) | set(x2.tolist())))
    if logy:
        ax.set_yscale("log")
    ax.legend()

    fig.tight_layout()
    fig.savefig(outpath(filename), dpi=300, bbox_inches="tight")
    plt.close(fig)


def focused_comparison(indices, labels, metric_async, metric_block, ylabel, title, filename, logy=False):
    sel_labels = [labels[i] for i in indices]
    a_vals = metric_async[indices]
    b_vals = metric_block[indices]
    grouped_bar_chart(sel_labels, a_vals, b_vals, ylabel, title, filename, logy=logy)


def main():
    ensure_outdir()

    async_summary_rows = load_rows(SUMMARY_ASYNC)
    block_summary_rows = load_rows(SUMMARY_BLOCK)

    async_fanout_rows = load_rows(FANOUT_ASYNC)
    block_fanout_rows = load_rows(FANOUT_BLOCK)

    async_fgrw_rows = load_rows(FGRW_ASYNC)
    block_fgrw_rows = load_rows(FGRW_BLOCK)

    async_data = summary_to_metric_arrays(async_summary_rows, WORKLOAD_ORDER)
    blocking_data = summary_to_metric_arrays(block_summary_rows, WORKLOAD_ORDER)

    pretty_labels = [PRETTY_LABELS[w] for w in WORKLOAD_ORDER]

    # 1. Throughput by workload
    grouped_bar_chart(
        pretty_labels,
        async_data["ops_per_sec"],
        blocking_data["ops_per_sec"],
        ylabel="Throughput (ops/s)",
        title="Throughput comparison across workloads",
        filename="fig_throughput_ops.png",
    )

    # Optional bandwidth figure
    grouped_bar_chart(
        pretty_labels,
        async_data["mib_per_sec"],
        blocking_data["mib_per_sec"],
        ylabel="Bandwidth (MiB/s)",
        title="Bandwidth comparison across workloads",
        filename="fig_bandwidth_mib.png",
    )

    # Optional average latency figure
    grouped_bar_chart(
        pretty_labels,
        async_data["avg_us"],
        blocking_data["avg_us"],
        ylabel="Average latency (us)",
        title="Average latency across workloads",
        filename="fig_avg_latency.png",
        logy=True,
    )

    # 2. p99 latency by workload
    grouped_bar_chart(
        pretty_labels,
        async_data["p99_us"],
        blocking_data["p99_us"],
        ylabel="p99 latency (us)",
        title="Tail latency (p99) across workloads",
        filename="fig_p99_latency.png",
        logy=True,
    )

    # Optional latency summary
    latency_triptych(
        pretty_labels,
        async_data["p50_us"],
        async_data["p95_us"],
        async_data["p99_us"],
        blocking_data["p50_us"],
        blocking_data["p95_us"],
        blocking_data["p99_us"],
        filename="fig_latency_summary.png",
    )

    # Optional speedup figures
    speedup_ops = async_data["ops_per_sec"] / blocking_data["ops_per_sec"]
    grouped_bar_chart_single_series(
        pretty_labels,
        speedup_ops,
        ylabel="AsyncMux speedup over BlockingMux",
        title="AsyncMux throughput speedup over BlockingMux",
        filename="fig_speedup_ops.png",
    )

    speedup_tail = blocking_data["p99_us"] / async_data["p99_us"]
    grouped_bar_chart_single_series(
        pretty_labels,
        speedup_tail,
        ylabel="BlockingMux p99 / AsyncMux p99",
        title="BlockingMux p99 / AsyncMux p99 (higher is better for AsyncMux)",
        filename="fig_speedup_p99.png",
    )

    # Optional focused summary figures
    read_heavy_indices = [1, 2, 5]
    write_heavy_indices = [0, 3, 4, 6]

    focused_comparison(
        read_heavy_indices,
        pretty_labels,
        async_data["ops_per_sec"],
        blocking_data["ops_per_sec"],
        ylabel="Throughput (ops/s)",
        title="Read-heavy workloads",
        filename="fig_read_heavy_ops.png",
    )

    focused_comparison(
        write_heavy_indices,
        pretty_labels,
        async_data["ops_per_sec"],
        blocking_data["ops_per_sec"],
        ylabel="Throughput (ops/s)",
        title="Write-heavy and coordination-heavy workloads",
        filename="fig_write_heavy_ops.png",
    )

    focused_comparison(
        write_heavy_indices,
        pretty_labels,
        async_data["p99_us"],
        blocking_data["p99_us"],
        ylabel="p99 latency (us)",
        title="Tail latency for write-heavy and coordination-heavy workloads",
        filename="fig_write_heavy_p99.png",
        logy=True,
    )

    # 3. Throughput vs concurrency for fanout
    fanout_async_x, fanout_async_ops = sweep_to_xy(
        async_fanout_rows, "fanout_read_multitier", "ops_per_sec"
    )
    fanout_block_x, fanout_block_ops = sweep_to_xy(
        block_fanout_rows, "fanout_read_multitier", "ops_per_sec"
    )

    line_chart_two_series(
        fanout_async_x,
        fanout_async_ops,
        fanout_block_x,
        fanout_block_ops,
        ylabel="Throughput (ops/s)",
        title="Fanout multitier read throughput vs concurrency",
        filename="fig_fanout_throughput_vs_concurrency.png",
    )

    # 4. p99 latency vs concurrency for fanout
    fanout_async_x, fanout_async_p99 = sweep_to_xy(
        async_fanout_rows, "fanout_read_multitier", "p99_us"
    )
    fanout_block_x, fanout_block_p99 = sweep_to_xy(
        block_fanout_rows, "fanout_read_multitier", "p99_us"
    )

    line_chart_two_series(
        fanout_async_x,
        fanout_async_p99,
        fanout_block_x,
        fanout_block_p99,
        ylabel="p99 latency (us)",
        title="Fanout multitier read p99 latency vs concurrency",
        filename="fig_fanout_p99_vs_concurrency.png",
        logy=True,
    )

    # 5. Throughput vs concurrency for foreground multitier RW
    fgrw_async_x, fgrw_async_ops = sweep_to_xy(
        async_fgrw_rows, "foreground_rw_multitier", "ops_per_sec"
    )
    fgrw_block_x, fgrw_block_ops = sweep_to_xy(
        block_fgrw_rows, "foreground_rw_multitier", "ops_per_sec"
    )

    line_chart_two_series(
        fgrw_async_x,
        fgrw_async_ops,
        fgrw_block_x,
        fgrw_block_ops,
        ylabel="Throughput (ops/s)",
        title="Foreground multitier RW throughput vs concurrency",
        filename="fig_foreground_rw_throughput_vs_concurrency.png",
    )

    print("Saved figures to:", OUTDIR)
    print("  fig_throughput_ops.png")
    print("  fig_bandwidth_mib.png")
    print("  fig_avg_latency.png")
    print("  fig_p99_latency.png")
    print("  fig_latency_summary.png")
    print("  fig_speedup_ops.png")
    print("  fig_speedup_p99.png")
    print("  fig_read_heavy_ops.png")
    print("  fig_write_heavy_ops.png")
    print("  fig_write_heavy_p99.png")
    print("  fig_fanout_throughput_vs_concurrency.png")
    print("  fig_fanout_p99_vs_concurrency.png")
    print("  fig_foreground_rw_throughput_vs_concurrency.png")


if __name__ == "__main__":
    main()