"""Genera grafici di scalabilita' a partire da metrics/<job>/results.csv.

Politica di aggregazione:
    - Filtra le righe con exit_code != 0 (run fallite).
    - Per ogni coppia (framework, size), SCARTA la prima run (warm-up:
      la prima invocazione paga il costo Tez localize / JVM cold start) e
      calcola la media delle restanti.
    - Se per una (framework, size) c'e' una sola run, la usa cosi' com'e'
      ma stampa un warning.

Output (in metrics/<job>/plots/):
    - walltime_by_size.png    : line chart wall_seconds vs size, per framework
    - walltime_bars.png       : bar chart raggruppato (size x framework)
    - speedup_vs_mr.png       : speedup relativo al baseline MapReduce
    - summary.md              : tabella markdown con i numeri aggregati

Uso:
    python3.11 bench/plot.py [job]   # job default: first_job
"""
import sys
from pathlib import Path

try:
    import pandas as pd
    import matplotlib.pyplot as plt
except ImportError as e:
    sys.exit(f"Dipendenza mancante: {e.name}. Installa con: pip3 install pandas matplotlib")


JOB = sys.argv[1] if len(sys.argv) > 1 else "first_job"

PROJECT_DIR = Path(__file__).resolve().parent.parent
CSV_PATH = PROJECT_DIR / "metrics" / JOB / "results.csv"
PLOTS_DIR = PROJECT_DIR / "metrics" / JOB / "plots"
SUMMARY_MD = PROJECT_DIR / "metrics" / JOB / "summary.md"

PLOTS_DIR.mkdir(parents=True, exist_ok=True)

# Colori coerenti per i 4 framework (pal. accessible).
COLORS = {
    "mapreduce":        "#d62728",  # rosso
    "hiveql":           "#ff7f0e",  # arancio
    "hiveql_pipelined": "#9467bd",  # viola (variante)
    "sparkcore":        "#2ca02c",  # verde
    "sparksql":         "#1f77b4",  # blu
}
FRAMEWORK_ORDER = ["mapreduce", "hiveql", "hiveql_pipelined", "sparkcore", "sparksql"]


def load_and_aggregate():
    df = pd.read_csv(CSV_PATH)
    df = df[df["exit_code"] == 0].copy()
    if df.empty:
        sys.exit(f"Nessuna run valida in {CSV_PATH}")

    # Scarta la prima run di ogni (framework, size).
    df["size"] = df["size"].astype(int)
    df_sorted = df.sort_values(["framework", "size", "run"])

    def aggregate(group):
        if len(group) == 1:
            print(f"  WARN: {group.iloc[0]['framework']} size={group.iloc[0]['size']} ha 1 sola run, uso quella")
            return group["wall_seconds"].iloc[0]
        return group["wall_seconds"].iloc[1:].mean()  # scarta la prima

    agg = (
        df_sorted.groupby(["framework", "size"])
                 .apply(aggregate)
                 .reset_index(name="wall_avg")
    )
    return agg


def plot_walltime_lines(agg):
    fig, ax = plt.subplots(figsize=(10, 6))
    for fw in FRAMEWORK_ORDER:
        sub = agg[agg["framework"] == fw].sort_values("size")
        if sub.empty:
            continue
        ax.plot(sub["size"], sub["wall_avg"], marker="o", color=COLORS[fw], linewidth=2, label=fw)
    ax.set_xlabel("Dimensione input (% del clean)")
    ax.set_ylabel("Wall-clock medio (secondi)")
    ax.set_title(f"Scalabilita' first_job - {JOB} - {len(agg['size'].unique())} size, media di N-1 run")
    ax.grid(True, alpha=0.3)
    ax.legend(loc="upper left")
    out = PLOTS_DIR / "walltime_by_size.png"
    fig.tight_layout()
    fig.savefig(out, dpi=140)
    plt.close(fig)
    print(f"  scritto {out}")


def plot_walltime_bars(agg):
    sizes = sorted(agg["size"].unique())
    fig, ax = plt.subplots(figsize=(11, 6))
    n_fw = len(FRAMEWORK_ORDER)
    bar_w = 0.8 / n_fw
    x = list(range(len(sizes)))
    for i, fw in enumerate(FRAMEWORK_ORDER):
        sub = agg[agg["framework"] == fw].set_index("size").reindex(sizes)
        offsets = [xi + (i - n_fw / 2 + 0.5) * bar_w for xi in x]
        ax.bar(offsets, sub["wall_avg"], width=bar_w, color=COLORS[fw], label=fw)
    ax.set_xticks(x)
    ax.set_xticklabels([f"{s}%" for s in sizes])
    ax.set_xlabel("Dimensione input")
    ax.set_ylabel("Wall-clock medio (secondi)")
    ax.set_title("Tempi di esecuzione per framework e dimensione input")
    ax.grid(True, axis="y", alpha=0.3)
    ax.legend()
    out = PLOTS_DIR / "walltime_bars.png"
    fig.tight_layout()
    fig.savefig(out, dpi=140)
    plt.close(fig)
    print(f"  scritto {out}")


def plot_speedup(agg):
    """Speedup di ciascun framework rispetto a MapReduce sulla stessa size."""
    pivot = agg.pivot(index="size", columns="framework", values="wall_avg")
    if "mapreduce" not in pivot.columns:
        print("  skip speedup_vs_mr.png: mapreduce mancante nei dati")
        return
    speedup = pivot.div(pivot["mapreduce"], axis=0).rdiv(1)  # mr_time / fw_time
    fig, ax = plt.subplots(figsize=(10, 6))
    for fw in FRAMEWORK_ORDER:
        if fw not in speedup.columns:
            continue
        ax.plot(speedup.index, speedup[fw], marker="o", color=COLORS[fw], linewidth=2, label=fw)
    ax.axhline(1.0, color="grey", linestyle="--", alpha=0.5)
    ax.set_xlabel("Dimensione input (% del clean)")
    ax.set_ylabel("Speedup vs MapReduce (>1 = piu' veloce)")
    ax.set_title("Speedup relativo al baseline MapReduce")
    ax.grid(True, alpha=0.3)
    ax.legend()
    out = PLOTS_DIR / "speedup_vs_mr.png"
    fig.tight_layout()
    fig.savefig(out, dpi=140)
    plt.close(fig)
    print(f"  scritto {out}")


def write_summary(agg):
    sizes = sorted(agg["size"].unique())
    lines = [
        f"# Riepilogo bench - {JOB}",
        "",
        f"Sorgente: `{CSV_PATH.relative_to(PROJECT_DIR)}`",
        "Politica: scartata la prima run (warm-up), media delle restanti.",
        "",
        "## Tempi medi wall-clock (secondi)",
        "",
        "| Framework | " + " | ".join(f"{s}%" for s in sizes) + " |",
        "|---" * (len(sizes) + 1) + "|",
    ]
    for fw in FRAMEWORK_ORDER:
        row = [fw]
        for s in sizes:
            v = agg[(agg["framework"] == fw) & (agg["size"] == s)]["wall_avg"]
            row.append(f"{v.iloc[0]:.1f}" if not v.empty else "-")
        lines.append("| " + " | ".join(row) + " |")
    lines.append("")
    SUMMARY_MD.write_text("\n".join(lines))
    print(f"  scritto {SUMMARY_MD}")


def main():
    print(f"=== Plotting {JOB} ===")
    agg = load_and_aggregate()
    print(f"Righe aggregate: {len(agg)}")
    print(agg.to_string(index=False))
    print()
    plot_walltime_lines(agg)
    plot_walltime_bars(agg)
    plot_speedup(agg)
    write_summary(agg)


if __name__ == "__main__":
    main()
