import os

import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np


OUTDIR = "figs"


# -----------------------------
# Benchmark data
# -----------------------------
workloads = [
    "sequential_write",
    "sequential_read",
    "random_read",
    "random_write",
    "mixed_80r_20w",
    "fanout_read_multitier",
    "multitier_rw",
]

pretty_labels = [
    "Seq Write",
    "Seq Read",
    "Rand Read",
    "Rand Write",
    "Mixed 80/20",
    "Fanout Read",
    "Multitier RW",
]

async_data = {
    "ops_s": np.array([1098.3, 165657.3, 150091.2, 246.9, 1200.4, 227182.4, 5098.2]),
    "mib_s": np.array([4.3, 647.1, 586.3, 1.0, 4.7, 887.4, 19.9]),
    "avg_us": np.array([14546.8, 96.1, 106.1, 64752.6, 13289.9, 70.0, 3132.7]),
    "p50_us": np.array([3915.2, 44.8, 60.4, 12569.2, 5611.7, 29.3, 1133.7]),
    "p95_us": np.array([14841.3, 65.4, 67.8, 21282.4, 13735.9, 55.1, 3573.8]),
    "p99_us": np.array([27057.9, 77.3, 78.4, 76936.7, 26687.9, 73.1, 4410.1]),
}

blocking_data = {
    "ops_s": np.array([677.3, 179235.6, 185185.0, 200.5, 919.5, 170192.8, 922.2]),
    "mib_s": np.array([2.6, 700.1, 723.4, 0.8, 3.6, 664.8, 3.6]),
    "avg_us": np.array([23578.6, 88.2, 85.4, 79756.2, 17396.6, 92.9, 17334.8]),
    "p50_us": np.array([12038.0, 60.8, 56.9, 31966.9, 10437.2, 75.5, 10351.7]),
    "p95_us": np.array([60827.7, 209.6, 204.8, 186818.1, 51454.6, 195.9, 46806.4]),
    "p99_us": np.array([124878.9, 309.9, 302.6, 766773.2, 148680.7, 274.1, 169663.4]),
}

# -----------------------------
# Helpers
# -----------------------------
def ensure_outdir() -> None:
    os.makedirs(OUTDIR, exist_ok=True)


def outpath(filename: str) -> str:
    return os.path.join(OUTDIR, filename)


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


def focused_comparison(indices, labels, metric_async, metric_block, ylabel, title, filename, logy=False):
    sel_labels = [labels[i] for i in indices]
    a_vals = metric_async[indices]
    b_vals = metric_block[indices]
    grouped_bar_chart(sel_labels, a_vals, b_vals, ylabel, title, filename, logy=logy)


def main():
    ensure_outdir()

    grouped_bar_chart(
        pretty_labels,
        async_data["ops_s"],
        blocking_data["ops_s"],
        ylabel="Throughput (ops/s)",
        title="Throughput comparison across workloads",
        filename="fig_throughput_ops.png",
    )

    grouped_bar_chart(
        pretty_labels,
        async_data["mib_s"],
        blocking_data["mib_s"],
        ylabel="Bandwidth (MiB/s)",
        title="Bandwidth comparison across workloads",
        filename="fig_bandwidth_mib.png",
    )

    grouped_bar_chart(
        pretty_labels,
        async_data["avg_us"],
        blocking_data["avg_us"],
        ylabel="Average latency (us)",
        title="Average latency across workloads",
        filename="fig_avg_latency.png",
        logy=True,
    )

    grouped_bar_chart(
        pretty_labels,
        async_data["p99_us"],
        blocking_data["p99_us"],
        ylabel="p99 latency (us)",
        title="Tail latency (p99) across workloads",
        filename="fig_p99_latency.png",
        logy=True,
    )

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

    speedup_ops = async_data["ops_s"] / blocking_data["ops_s"]
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

    read_heavy_indices = [1, 2, 5]
    write_heavy_indices = [0, 3, 4, 6]

    focused_comparison(
        read_heavy_indices,
        pretty_labels,
        async_data["ops_s"],
        blocking_data["ops_s"],
        ylabel="Throughput (ops/s)",
        title="Read-heavy workloads",
        filename="fig_read_heavy_ops.png",
    )

    focused_comparison(
        write_heavy_indices,
        pretty_labels,
        async_data["ops_s"],
        blocking_data["ops_s"],
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
    print()
    print("Note: A true CDF plot requires raw latency samples, not only p50/p95/p99 summaries.")


if __name__ == "__main__":
    main()