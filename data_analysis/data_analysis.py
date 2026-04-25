import matplotlib.pyplot as plt
import numpy as np

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
    "ops_s": np.array([699.3, 177091.0, 154636.7, 197.5, 906.0, 162929.9, 936.7]),
    "mib_s": np.array([2.7, 691.8, 604.0, 0.8, 3.5, 636.4, 3.7]),
    "avg_us": np.array([22839.3, 89.2, 102.5, 80960.3, 17650.2, 97.0, 17061.4]),
    "p50_us": np.array([10923.3, 60.1, 70.0, 36379.4, 10420.2, 79.6, 10519.8]),
    "p95_us": np.array([60835.9, 192.1, 243.2, 182639.8, 44055.7, 204.6, 46073.2]),
    "p99_us": np.array([189063.6, 289.5, 358.0, 836231.3, 181919.3, 318.0, 150925.3]),
}

# -----------------------------
# Helpers
# -----------------------------
def grouped_bar_chart(labels, a_vals, b_vals, ylabel, title, filename, logy=False):
    x = np.arange(len(labels))
    width = 0.38

    plt.figure(figsize=(10, 4.8))
    plt.bar(x - width / 2, a_vals, width, label="AsyncMux")
    plt.bar(x + width / 2, b_vals, width, label="BlockingMux")
    plt.xticks(x, labels, rotation=25, ha="right")
    plt.ylabel(ylabel)
    plt.title(title)
    if logy:
        plt.yscale("log")
    plt.legend()
    plt.tight_layout()
    plt.savefig(filename, dpi=300, bbox_inches="tight")
    plt.close()


def grouped_bar_chart_speedup(labels, speedup_vals, title, filename):
    x = np.arange(len(labels))
    plt.figure(figsize=(10, 4.8))
    plt.bar(x, speedup_vals)
    plt.axhline(1.0, linestyle="--", linewidth=1)
    plt.xticks(x, labels, rotation=25, ha="right")
    plt.ylabel("AsyncMux speedup over BlockingMux")
    plt.title(title)
    plt.tight_layout()
    plt.savefig(filename, dpi=300, bbox_inches="tight")
    plt.close()


def latency_triptych(labels, a_p50, a_p95, a_p99, b_p50, b_p95, b_p99, filename):
    x = np.arange(len(labels))
    width = 0.13

    plt.figure(figsize=(12, 5.2))
    plt.bar(x - 2.5 * width, a_p50, width, label="Async p50")
    plt.bar(x - 1.5 * width, a_p95, width, label="Async p95")
    plt.bar(x - 0.5 * width, a_p99, width, label="Async p99")
    plt.bar(x + 0.5 * width, b_p50, width, label="Blocking p50")
    plt.bar(x + 1.5 * width, b_p95, width, label="Blocking p95")
    plt.bar(x + 2.5 * width, b_p99, width, label="Blocking p99")

    plt.xticks(x, labels, rotation=25, ha="right")
    plt.ylabel("Latency (us)")
    plt.yscale("log")
    plt.title("Latency distribution summary by workload")
    plt.legend(ncol=3)
    plt.tight_layout()
    plt.savefig(filename, dpi=300, bbox_inches="tight")
    plt.close()


def focused_comparison(indices, labels, metric_async, metric_block, ylabel, title, filename, logy=False):
    sel_labels = [labels[i] for i in indices]
    a_vals = metric_async[indices]
    b_vals = metric_block[indices]
    grouped_bar_chart(sel_labels, a_vals, b_vals, ylabel, title, filename, logy=logy)


# -----------------------------
# Generate figures
# -----------------------------
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
grouped_bar_chart_speedup(
    pretty_labels,
    speedup_ops,
    title="AsyncMux throughput speedup over BlockingMux",
    filename="fig_speedup_ops.png",
)

speedup_tail = blocking_data["p99_us"] / async_data["p99_us"]
grouped_bar_chart_speedup(
    pretty_labels,
    speedup_tail,
    title="BlockingMux p99 / AsyncMux p99 (higher is better for AsyncMux)",
    filename="fig_speedup_p99.png",
)

# Optional focused figures for the paper narrative
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

print("Saved figures:")
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