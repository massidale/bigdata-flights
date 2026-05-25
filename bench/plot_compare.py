"""Confronto locale vs cluster per il first_job.

Legge:
    - metrics/first_job/results.csv      (run locale)
    - metrics/first_job_emr/results.csv  (run su EMR)

Produce in metrics/first_job/plots/:
    - compare_local_vs_emr.png  (line chart side-by-side per framework)
    - compare_speedup.png       (rapporto locale/emr per ogni framework × size)
    - compare_summary.md        (tabella markdown finale)
"""
import sys
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt

PROJECT_DIR = Path(__file__).resolve().parent.parent
LOCAL_CSV = PROJECT_DIR / "metrics" / "first_job" / "results.csv"
EMR_CSV = PROJECT_DIR / "metrics" / "first_job_emr" / "results.csv"
PLOTS_DIR = PROJECT_DIR / "metrics" / "first_job" / "plots"
SUMMARY_MD = PROJECT_DIR / "metrics" / "first_job" / "compare_summary.md"

PLOTS_DIR.mkdir(parents=True, exist_ok=True)

COLORS = {
    "mapreduce":        "#d62728",
    "hiveql":           "#ff7f0e",
    "hiveql_pipelined": "#9467bd",
    "sparkcore":        "#2ca02c",
    "sparksql":         "#1f77b4",
}
# Per il confronto cluster, su EMR usiamo solo Hive pipelined (la tesi e' quella).
FRAMEWORKS_COMPARE = ["mapreduce", "hiveql_pipelined", "sparkcore", "sparksql"]


def aggregate(csv_path):
    df = pd.read_csv(csv_path)
    df = df[df["exit_code"] == 0].copy()
    df["size"] = df["size"].astype(int)
    # se ci sono piu' run per (framework,size), scarta la prima (warm-up) e media
    def agg(g):
        return g["wall_seconds"].iloc[1:].mean() if len(g) > 1 else g["wall_seconds"].iloc[0]
    out = df.groupby(["framework", "size"]).apply(agg, include_groups=False).reset_index(name="wall")
    return out


def plot_compare_lines(local, emr):
    fig, axes = plt.subplots(1, 2, figsize=(14, 6), sharey=True)
    for ax, df, title in ((axes[0], local, "Locale (Mac single-node)"),
                          (axes[1], emr, "EMR (1 master + 4 core m5.xlarge)")):
        for fw in FRAMEWORKS_COMPARE:
            sub = df[df["framework"] == fw].sort_values("size")
            if sub.empty:
                continue
            ax.plot(sub["size"], sub["wall"], marker="o", linewidth=2,
                    color=COLORS[fw], label=fw)
        ax.set_xlabel("Dimensione input (% del clean)")
        ax.set_title(title)
        ax.grid(True, alpha=0.3)
        ax.legend()
    axes[0].set_ylabel("Wall-clock medio (secondi)")
    fig.suptitle("first_job - scalabilita' locale vs EMR")
    out = PLOTS_DIR / "compare_local_vs_emr.png"
    fig.tight_layout()
    fig.savefig(out, dpi=140)
    plt.close(fig)
    print(f"  scritto {out}")


def plot_compare_ratio(local, emr):
    # Per ogni (framework, size): ratio = emr / local (>1 = EMR piu' lento)
    merged = local.merge(emr, on=["framework", "size"], suffixes=("_local", "_emr"))
    merged = merged[merged["framework"].isin(FRAMEWORKS_COMPARE)]
    merged["ratio"] = merged["wall_emr"] / merged["wall_local"]

    fig, ax = plt.subplots(figsize=(10, 6))
    for fw in FRAMEWORKS_COMPARE:
        sub = merged[merged["framework"] == fw].sort_values("size")
        if sub.empty:
            continue
        ax.plot(sub["size"], sub["ratio"], marker="o", color=COLORS[fw], linewidth=2, label=fw)
    ax.axhline(1.0, color="grey", linestyle="--", alpha=0.5, label="parita'")
    ax.set_xlabel("Dimensione input (% del clean)")
    ax.set_ylabel("EMR / Locale  (>1 = cluster piu' lento)")
    ax.set_title("Overhead EMR rispetto a locale per framework e dimensione")
    ax.grid(True, alpha=0.3)
    ax.legend()
    out = PLOTS_DIR / "compare_speedup.png"
    fig.tight_layout()
    fig.savefig(out, dpi=140)
    plt.close(fig)
    print(f"  scritto {out}")


def write_summary(local, emr):
    merged = local.merge(emr, on=["framework", "size"], suffixes=("_local", "_emr"), how="outer")
    merged = merged[merged["framework"].isin(FRAMEWORKS_COMPARE)]
    sizes = sorted(merged["size"].unique())
    lines = [
        "# first_job - confronto Locale vs EMR",
        "",
        "## Tempi medi wall-clock (secondi)",
        "",
        "**Locale** (Mac single-node, Spark local[*])",
        "",
        "| Framework | " + " | ".join(f"{s}%" for s in sizes) + " |",
        "|---" * (len(sizes) + 1) + "|",
    ]
    for fw in FRAMEWORKS_COMPARE:
        row = [fw]
        for s in sizes:
            v = merged[(merged["framework"] == fw) & (merged["size"] == s)]["wall_local"]
            row.append(f"{v.iloc[0]:.1f}" if not v.empty and pd.notna(v.iloc[0]) else "-")
        lines.append("| " + " | ".join(row) + " |")

    lines += ["", "**EMR** (1 master + 4 core m5.xlarge, Spark on YARN, Hive on Tez)", "",
              "| Framework | " + " | ".join(f"{s}%" for s in sizes) + " |",
              "|---" * (len(sizes) + 1) + "|"]
    for fw in FRAMEWORKS_COMPARE:
        row = [fw]
        for s in sizes:
            v = merged[(merged["framework"] == fw) & (merged["size"] == s)]["wall_emr"]
            row.append(f"{v.iloc[0]:.1f}" if not v.empty and pd.notna(v.iloc[0]) else "-")
        lines.append("| " + " | ".join(row) + " |")

    SUMMARY_MD.write_text("\n".join(lines))
    print(f"  scritto {SUMMARY_MD}")


def main():
    local = aggregate(LOCAL_CSV)
    emr = aggregate(EMR_CSV)
    print("=== locale ===")
    print(local.to_string(index=False))
    print()
    print("=== emr ===")
    print(emr.to_string(index=False))
    print()
    plot_compare_lines(local, emr)
    plot_compare_ratio(local, emr)
    write_summary(local, emr)


if __name__ == "__main__":
    main()
