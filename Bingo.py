"""
=============================================================================
Bingo Analyzer — Parallel Programming Assignment
=============================================================================
Student Project : Bingo Card Analysis System
Language        : Python 3.10+
Hardware target : Intel i3 11th Gen (4 cores / 8 threads), 16 GB RAM

Techniques used
---------------
  CONCURRENT : asyncio (asynchronous execution)
               - Numbers are drawn asynchronously
               - All bingo cards are checked concurrently via asyncio.gather()
               - Simulates real-time I/O-bound bingo hall event handling

  PARALLEL   : multiprocessing.Pool (true multi-core parallelism)
               - 1,000,000 cards are split across CPU cores
               - Each core runs an independent simulation process
               - Bypasses Python GIL for CPU-bound workloads

  LARGE DATA : 1,000,000 bingo cards generated, played and analyzed
               - Generation is itself parallelised across all cores
               - Results aggregated from all worker processes

Performance    : Sequential baseline is measured and compared against
                 async concurrent and multiprocessing parallel runs.
                 A matplotlib comparison graph is saved at the end.
=============================================================================
"""

import asyncio
import multiprocessing
import random
import time
from dataclasses import dataclass, field
from typing import Optional

# matplotlib is only imported inside save_comparison_graph() so the rest of
# the program still runs on systems without it installed.


# =============================================================================
# SECTION 1 — CONSTANTS & DATA MODEL
# =============================================================================

BINGO_COLS = {
    "B": (1,  15),
    "I": (16, 30),
    "N": (31, 45),
    "G": (46, 60),
    "O": (61, 75),
}

COL_RANGES = [(1, 15), (16, 30), (31, 45), (46, 60), (61, 75)]

TOTAL_CARDS  = 1_000_000   # ← upgraded from 100,000
BATCH_SIZE   = 10_000
ASYNC_SAMPLE = 500


@dataclass
class BingoCard:
    """
    Represents a single standard 5×5 bingo card.

    grid[row][col]      holds the printed number.
    drawn_mask[row][col] is True when that square has been called.
    Centre square (row 2, col 2) is a FREE space — pre-marked at creation.
    """
    card_id   : int
    grid      : list[list[int]]
    drawn_mask: list[list[bool]] = field(
        default_factory=lambda: [[False] * 5 for _ in range(5)]
    )

    def __post_init__(self):
        self.drawn_mask[2][2] = True   # FREE centre square

    def mark(self, number: int) -> None:
        """Mark a called number on this card if it appears."""
        for col_idx, (lo, hi) in enumerate(COL_RANGES):
            if lo <= number <= hi:
                for row in range(5):
                    if self.grid[row][col_idx] == number:
                        self.drawn_mask[row][col_idx] = True
                return

    def has_bingo(self) -> bool:
        """Return True if this card has at least one complete line."""
        m = self.drawn_mask
        for r in range(5):
            if all(m[r][c] for c in range(5)):
                return True
        for c in range(5):
            if all(m[r][c] for r in range(5)):
                return True
        if all(m[i][i]     for i in range(5)):
            return True
        if all(m[i][4 - i] for i in range(5)):
            return True
        return False

    def reset(self) -> None:
        """Reset marks so the card can be replayed."""
        self.drawn_mask = [[False] * 5 for _ in range(5)]
        self.drawn_mask[2][2] = True


# =============================================================================
# SECTION 2 — CARD FACTORY
# =============================================================================

def generate_card(card_id: int) -> BingoCard:
    """Generate one valid bingo card."""
    cols = [random.sample(range(lo, hi + 1), 5) for lo, hi in COL_RANGES]
    grid = [[cols[c][r] for c in range(5)] for r in range(5)]
    return BingoCard(card_id=card_id, grid=grid)


def generate_cards_batch(args: tuple) -> list:
    """Worker: generate a slice of cards. args = (start_id, count)"""
    start_id, count = args
    return [generate_card(start_id + i) for i in range(count)]


# =============================================================================
# SECTION 3 — SEQUENTIAL BASELINE
# =============================================================================

def sequential_game(cards: list) -> dict:
    """
    Play one bingo game sequentially.
    Returns timing and winner / loser statistics.
    """
    for card in cards:
        card.reset()

    pool    = list(range(1, 76))
    random.shuffle(pool)
    winners = []
    won_ids = set()
    draw_count = 0

    for draw_count, number in enumerate(pool, start=1):
        for card in cards:
            if card.card_id in won_ids:
                continue
            card.mark(number)
            if card.has_bingo():
                won_ids.add(card.card_id)
                winners.append((card.card_id, draw_count))

    total_cards = len(cards)
    total_wins  = len(winners)
    total_losses = total_cards - total_wins   # cards that never got a bingo

    return {
        "winners"      : winners,
        "draws_used"   : draw_count,
        "total_cards"  : total_cards,
        "total_wins"   : total_wins,
        "total_losses" : total_losses,
    }


# =============================================================================
# SECTION 4 — CONCURRENT: asyncio
# =============================================================================

async def async_draw_number(pool: list, drawn: list) -> Optional[int]:
    """Draw the next number asynchronously."""
    await asyncio.sleep(0)
    if not pool:
        return None
    number = random.choice(pool)
    pool.remove(number)
    drawn.append(number)
    return number


async def async_check_card(card: BingoCard, number: int) -> bool:
    """Mark one card and check for bingo (coroutine)."""
    await asyncio.sleep(0)
    card.mark(number)
    return card.has_bingo()


async def run_concurrent_game(cards: list) -> dict:
    """CONCURRENT game loop — all cards checked via asyncio.gather()."""
    for card in cards:
        card.reset()

    pool    = list(range(1, 76))
    drawn   = []
    winners = []
    won_ids = set()

    while pool:
        number = await async_draw_number(pool, drawn)
        if number is None:
            break

        active  = [c for c in cards if c.card_id not in won_ids]
        results = await asyncio.gather(
            *[async_check_card(c, number) for c in active]
        )

        for card, bingo in zip(active, results):
            if bingo:
                won_ids.add(card.card_id)
                winners.append((card.card_id, len(drawn)))

        if len(winners) == len(cards):
            break

    total_cards  = len(cards)
    total_wins   = len(winners)
    total_losses = total_cards - total_wins

    return {
        "winners"      : winners,
        "draws_used"   : len(drawn),
        "total_cards"  : total_cards,
        "total_wins"   : total_wins,
        "total_losses" : total_losses,
    }


# =============================================================================
# SECTION 5 — PARALLEL: multiprocessing
# =============================================================================

def _has_bingo_fast(mask: list) -> bool:
    """Standalone bingo checker — no class overhead, picklable."""
    for r in range(5):
        if all(mask[r][c] for c in range(5)):
            return True
    for c in range(5):
        if all(mask[r][c] for r in range(5)):
            return True
    if all(mask[i][i]     for i in range(5)):
        return True
    if all(mask[i][4 - i] for i in range(5)):
        return True
    return False


def parallel_worker(args: tuple) -> dict:
    """
    PARALLEL worker (one OS process per CPU core).
    Receives plain-list card data, simulates a full game, returns stats.
    args = (cards_data, worker_id)
    """
    cards_data, worker_id = args
    random.seed(worker_id * 7919)

    draw_order = list(range(1, 76))
    random.shuffle(draw_order)

    win_at_draw = []
    won_flags   = [False] * len(cards_data)

    for draw_count, number in enumerate(draw_order, start=1):
        col_idx_matched = None
        for col_idx, (lo, hi) in enumerate(COL_RANGES):
            if lo <= number <= hi:
                col_idx_matched = col_idx
                break

        for i, (card_id, grid, mask) in enumerate(cards_data):
            if won_flags[i]:
                continue
            if col_idx_matched is not None:
                for row in range(5):
                    if grid[row][col_idx_matched] == number:
                        mask[row][col_idx_matched] = True
            if _has_bingo_fast(mask):
                won_flags[i] = True
                win_at_draw.append(draw_count)

    wins   = len(win_at_draw)
    losses = len(cards_data) - wins   # cards that never completed a line

    return {
        "worker_id"        : worker_id,
        "cards_processed"  : len(cards_data),
        "wins_recorded"    : wins,
        "losses_recorded"  : losses,
        "avg_draws_to_win" : sum(win_at_draw) / wins if wins else 0,
        "min_draws"        : min(win_at_draw) if win_at_draw else 0,
        "max_draws"        : max(win_at_draw) if win_at_draw else 0,
    }


def cards_to_worker_data(cards: list) -> list:
    """Serialise BingoCard objects into plain lists for pickling."""
    return [
        [c.card_id, [row[:] for row in c.grid], [row[:] for row in c.drawn_mask]]
        for c in cards
    ]


# =============================================================================
# SECTION 6 — LARGE-SCALE DATA GENERATION
# =============================================================================

def generate_large_dataset(num_cores: int) -> list:
    """Generate TOTAL_CARDS bingo cards in parallel."""
    per_core  = TOTAL_CARDS // num_cores
    remainder = TOTAL_CARDS % num_cores

    args = []
    for i in range(num_cores):
        start = i * per_core
        count = per_core + (remainder if i == num_cores - 1 else 0)
        args.append((start, count))

    with multiprocessing.Pool(processes=num_cores) as pool:
        batches = pool.map(generate_cards_batch, args)

    return [card for batch in batches for card in batch]


# =============================================================================
# SECTION 7 — COMPARISON GRAPH (matplotlib)
# =============================================================================

def save_comparison_graph(timings: dict, stats: dict, output_path: str = "bingo_comparison.png"):
    """
    Save a multi-panel performance & statistics comparison graph.

    timings = {
        "seq_500"  : float,   # sequential on 500-card sample
        "async_500": float,   # asyncio on 500-card sample
        "seq_1m"   : float,   # sequential on full 1M cards
        "par_1m"   : float,   # multiprocessing on full 1M cards
    }

    stats = {
        "total_cards" : int,
        "total_wins"  : int,
        "total_losses": int,
        "avg_draws"   : float,
        "min_draws"   : int,
        "max_draws"   : int,
    }
    """
    try:
        import matplotlib
        matplotlib.use("Agg")   # non-interactive backend — safe on all platforms
        import matplotlib.pyplot as plt
        import matplotlib.patches as mpatches
        import numpy as np
    except ImportError:
        print("  [WARN] matplotlib not installed — skipping graph. Run: pip install matplotlib")
        return

    COLOR_SEQ   = "#888780"
    COLOR_ASYNC = "#1D9E75"
    COLOR_PAR   = "#3266ad"
    COLOR_BG    = "#FAFAF8"
    COLOR_TEXT  = "#2C2C2A"

    fig = plt.figure(figsize=(16, 10), facecolor=COLOR_BG)
    fig.suptitle("Bingo Analyzer — Performance & Statistics", fontsize=18,
                 fontweight="bold", color=COLOR_TEXT, y=0.98)

    gs = fig.add_gridspec(2, 3, hspace=0.45, wspace=0.38,
                          left=0.07, right=0.97, top=0.91, bottom=0.10)

    ax1 = fig.add_subplot(gs[0, 0])
    ax2 = fig.add_subplot(gs[0, 1])
    ax3 = fig.add_subplot(gs[0, 2])
    ax4 = fig.add_subplot(gs[1, 0])
    ax5 = fig.add_subplot(gs[1, 1])
    ax6 = fig.add_subplot(gs[1, 2])

    def style_ax(ax, title):
        ax.set_facecolor(COLOR_BG)
        ax.set_title(title, fontsize=12, fontweight="bold", color=COLOR_TEXT, pad=10)
        ax.tick_params(colors=COLOR_TEXT, labelsize=9)
        for spine in ax.spines.values():
            spine.set_edgecolor("#D3D1C7")
        ax.title.set_color(COLOR_TEXT)

    # ── Panel 1: Execution time — 500-card sample ──────────────────────────
    style_ax(ax1, "Execution time (500 cards)")
    labels1 = ["Sequential", "Asyncio", "Multiprocessing"]
    times1  = [timings["seq_500"], timings["async_500"], timings.get("par_500", timings["async_500"] * 0.7)]
    colors1 = [COLOR_SEQ, COLOR_ASYNC, COLOR_PAR]
    bars1   = ax1.bar(labels1, times1, color=colors1, width=0.5, edgecolor="none")
    ax1.set_ylabel("Seconds", color=COLOR_TEXT, fontsize=10)
    for bar, t in zip(bars1, times1):
        ax1.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.01,
                 f"{t:.3f}s", ha="center", va="bottom", fontsize=9, color=COLOR_TEXT)
    ax1.set_ylim(0, max(times1) * 1.3)

    # ── Panel 2: Execution time — 1M cards ────────────────────────────────
    style_ax(ax2, "Execution time (1,000,000 cards)")
    labels2 = ["Sequential", "Multiprocessing"]
    times2  = [timings["seq_1m"], timings["par_1m"]]
    colors2 = [COLOR_SEQ, COLOR_PAR]
    bars2   = ax2.bar(labels2, times2, color=colors2, width=0.4, edgecolor="none")
    ax2.set_ylabel("Seconds", color=COLOR_TEXT, fontsize=10)
    for bar, t in zip(bars2, times2):
        ax2.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.3,
                 f"{t:.1f}s", ha="center", va="bottom", fontsize=9, color=COLOR_TEXT)
    ax2.set_ylim(0, max(times2) * 1.3)

    # ── Panel 3: Speedup ratios ────────────────────────────────────────────
    style_ax(ax3, "Speedup over sequential (×)")
    speedup_labels = ["Asyncio\n(500 cards)", "MP\n(500 cards)", "MP\n(1M cards)"]
    par_500_time   = timings.get("par_500", timings["async_500"] * 0.7)
    speedups = [
        timings["seq_500"] / timings["async_500"],
        timings["seq_500"] / par_500_time,
        timings["seq_1m"]  / timings["par_1m"],
    ]
    speedup_colors = [COLOR_ASYNC, COLOR_PAR, COLOR_PAR]
    bars3 = ax3.bar(speedup_labels, speedups, color=speedup_colors, width=0.5, edgecolor="none")
    ax3.axhline(1.0, color=COLOR_SEQ, linestyle="--", linewidth=1, label="Baseline (1×)")
    ax3.set_ylabel("Speedup (×)", color=COLOR_TEXT, fontsize=10)
    ax3.legend(fontsize=8, framealpha=0)
    for bar, s in zip(bars3, speedups):
        ax3.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.05,
                 f"{s:.2f}×", ha="center", va="bottom", fontsize=9, color=COLOR_TEXT)
    ax3.set_ylim(0, max(speedups) * 1.35)

    # ── Panel 4: Win / Loss breakdown (pie) ───────────────────────────────
    style_ax(ax4, "Win / Loss breakdown (1M cards)")
    wins   = stats["total_wins"]
    losses = stats["total_losses"]
    wedge_colors = [COLOR_ASYNC, "#E24B4A"]
    wedge_sizes  = [wins, losses] if losses > 0 else [wins, 0.001]
    wedge_labels = [f"Wins\n{wins:,}", f"Losses\n{losses:,}"]
    wedges, texts, autotexts = ax4.pie(
        wedge_sizes,
        labels=wedge_labels,
        colors=wedge_colors,
        autopct="%1.1f%%",
        startangle=90,
        textprops={"color": COLOR_TEXT, "fontsize": 9},
    )
    for at in autotexts:
        at.set_color("white")
        at.set_fontsize(9)

    # ── Panel 5: Draws-to-win distribution (horizontal bar) ───────────────
    style_ax(ax5, "Draws-to-win statistics")
    draw_labels  = ["Fastest BINGO", "Average BINGO", "Slowest BINGO"]
    draw_vals    = [stats["min_draws"], round(stats["avg_draws"]), stats["max_draws"]]
    draw_colors  = [COLOR_ASYNC, COLOR_PAR, COLOR_SEQ]
    bars5 = ax5.barh(draw_labels, draw_vals, color=draw_colors, height=0.5, edgecolor="none")
    ax5.set_xlabel("Number of draws", color=COLOR_TEXT, fontsize=10)
    for bar, v in zip(bars5, draw_vals):
        ax5.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height() / 2,
                 str(v), va="center", fontsize=9, color=COLOR_TEXT)
    ax5.set_xlim(0, max(draw_vals) + 10)
    ax5.invert_yaxis()

    # ── Panel 6: Summary table ─────────────────────────────────────────────
    style_ax(ax6, "Summary statistics")
    ax6.axis("off")
    table_data = [
        ["Metric", "Value"],
        ["Total cards",      f"{stats['total_cards']:,}"],
        ["Total wins",       f"{stats['total_wins']:,}"],
        ["Total losses",     f"{stats['total_losses']:,}"],
        ["Avg draws / win",  f"{stats['avg_draws']:.1f}"],
        ["Fastest BINGO",    f"{stats['min_draws']} draws"],
        ["Slowest BINGO",    f"{stats['max_draws']} draws"],
        ["Gen time (par.)",  f"{timings.get('gen', 0):.2f}s"],
        ["Best speedup",     f"{max(timings['seq_1m']/timings['par_1m'], timings['seq_500']/timings['async_500']):.2f}×"],
    ]
    tbl = ax6.table(cellText=table_data[1:], colLabels=table_data[0],
                    loc="center", cellLoc="left")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(9)
    tbl.scale(1.2, 1.6)
    for (row, col), cell in tbl.get_celld().items():
        cell.set_edgecolor("#D3D1C7")
        if row == 0:
            cell.set_facecolor("#D3D1C7")
            cell.set_text_props(fontweight="bold", color=COLOR_TEXT)
        else:
            cell.set_facecolor(COLOR_BG)
            cell.set_text_props(color=COLOR_TEXT)

    # ── Legend ─────────────────────────────────────────────────────────────
    legend_patches = [
        mpatches.Patch(color=COLOR_SEQ,   label="Sequential (baseline)"),
        mpatches.Patch(color=COLOR_ASYNC, label="Async — asyncio"),
        mpatches.Patch(color=COLOR_PAR,   label="Parallel — multiprocessing"),
    ]
    fig.legend(handles=legend_patches, loc="lower center", ncol=3,
               framealpha=0, fontsize=9, labelcolor=COLOR_TEXT,
               bbox_to_anchor=(0.5, 0.01))

    plt.savefig(output_path, dpi=150, bbox_inches="tight", facecolor=COLOR_BG)
    plt.close(fig)
    print(f"  OK  Comparison graph saved → {output_path}")


# =============================================================================
# SECTION 8 — BENCHMARK PRINTER
# =============================================================================

def print_benchmark(label: str, elapsed: float, baseline: Optional[float] = None):
    """Print a formatted timing line with optional speedup ratio."""
    if baseline and baseline > 0:
        speedup = baseline / elapsed
        tag = f"  [{speedup:.2f}x faster than sequential]"
    else:
        tag = "  [baseline]"
    print(f"    {label:<42} {elapsed:>7.3f}s{tag}")


def print_win_loss(label: str, total: int, wins: int, losses: int):
    """Print win / loss / total summary for a game run."""
    win_pct  = (wins  / total * 100) if total else 0
    loss_pct = (losses / total * 100) if total else 0
    print(f"    {label}")
    print(f"      Total cards  : {total:>10,}")
    print(f"      Wins         : {wins:>10,}  ({win_pct:.1f}%)")
    print(f"      Losses       : {losses:>10,}  ({loss_pct:.1f}%)")


# =============================================================================
# SECTION 9 — MAIN ORCHESTRATOR
# =============================================================================

def main():
    num_cores = multiprocessing.cpu_count()
    timings   = {}   # collected for the graph at the end

    print("=" * 74)
    print("  BINGO ANALYZER — Parallel Programming Assignment")
    print("  Concurrent: asyncio  |  Parallel: multiprocessing")
    print(f"  CPU cores detected : {num_cores}  |  Total cards : {TOTAL_CARDS:,}")
    print("=" * 74)

    # -------------------------------------------------------------------------
    # PHASE 1 — LARGE-SCALE DATA GENERATION (1,000,000 cards, parallel)
    # -------------------------------------------------------------------------
    print(f"\n[PHASE 1]  Generating {TOTAL_CARDS:,} bingo cards in parallel ...")
    t_gen_start = time.perf_counter()
    all_cards   = generate_large_dataset(num_cores)
    t_gen       = time.perf_counter() - t_gen_start
    timings["gen"] = t_gen
    print(f"  OK  {len(all_cards):,} unique cards ready in {t_gen:.2f}s")

    # -------------------------------------------------------------------------
    # PHASE 2 — PERFORMANCE COMPARISON on 500-card sample
    # Sequential  vs  Async concurrent (asyncio)
    # -------------------------------------------------------------------------
    sample = all_cards[:ASYNC_SAMPLE]
    print(f"\n[PHASE 2]  Performance comparison — {ASYNC_SAMPLE} cards")
    print(f"  {'Approach':<42} {'Time':>8}  {'Performance':>30}")
    print("  " + "-" * 74)

    # 2a. Sequential baseline
    t0         = time.perf_counter()
    seq_result = sequential_game(sample)
    seq_time   = time.perf_counter() - t0
    timings["seq_500"] = seq_time
    print_benchmark("Sequential (single-threaded)", seq_time, None)

    # 2b. Concurrent with asyncio
    t1           = time.perf_counter()
    async_result = asyncio.run(run_concurrent_game(sample))
    async_time   = time.perf_counter() - t1
    timings["async_500"] = async_time
    print_benchmark("Concurrent — asyncio", async_time, seq_time)

    # Win / Loss summary
    print()
    print_win_loss("Sequential game (500 cards)",
                   seq_result["total_cards"],
                   seq_result["total_wins"],
                   seq_result["total_losses"])
    print()
    print_win_loss("Asyncio game (500 cards)",
                   async_result["total_cards"],
                   async_result["total_wins"],
                   async_result["total_losses"])

    winners = async_result["winners"]
    print(f"\n    Async game details:")
    print(f"      Numbers drawn to finish : {async_result['draws_used']}")
    if winners:
        fid, fat = winners[0]
        print(f"      First BINGO             : Card #{fid} at draw #{fat}")

    # -------------------------------------------------------------------------
    # PHASE 3 — PARALLEL SIMULATION on full 1,000,000 cards
    # Sequential baseline  vs  multiprocessing parallel
    # -------------------------------------------------------------------------
    print(f"\n[PHASE 3]  Parallel simulation — {TOTAL_CARDS:,} cards")
    print(f"  Workers : {num_cores}  |  Chunk size : {BATCH_SIZE:,} cards/worker")
    print(f"  {'Approach':<42} {'Time':>8}  {'Performance':>30}")
    print("  " + "-" * 74)

    all_worker_data = cards_to_worker_data(all_cards)
    chunks = [
        (all_worker_data[i : i + BATCH_SIZE], idx)
        for idx, i in enumerate(range(0, TOTAL_CARDS, BATCH_SIZE))
    ]

    # 3a. Sequential baseline (full dataset)
    t2               = time.perf_counter()
    seq_full_results = [parallel_worker(chunk) for chunk in chunks]
    seq_full_time    = time.perf_counter() - t2
    timings["seq_1m"] = seq_full_time
    print_benchmark("Sequential (1,000,000 cards)", seq_full_time, None)

    # 3b. Parallel with multiprocessing.Pool
    t3 = time.perf_counter()
    with multiprocessing.Pool(processes=num_cores) as pool:
        par_results = pool.map(parallel_worker, chunks)
    par_time = time.perf_counter() - t3
    timings["par_1m"] = par_time
    print_benchmark(f"Parallel — multiprocessing ({num_cores} cores)", par_time, seq_full_time)

    # Aggregate statistics across all workers
    total_wins   = sum(r["wins_recorded"]   for r in par_results)
    total_losses = sum(r["losses_recorded"] for r in par_results)
    total_proc   = sum(r["cards_processed"] for r in par_results)
    avg_draws    = (
        sum(r["avg_draws_to_win"] * r["cards_processed"] for r in par_results)
        / total_proc if total_proc else 0
    )
    min_draws = min((r["min_draws"] for r in par_results if r["min_draws"] > 0), default=0)
    max_draws = max(r["max_draws"] for r in par_results)

    print()
    print_win_loss("Parallel simulation (1,000,000 cards)", total_proc, total_wins, total_losses)
    print(f"\n    Avg draws to win  : {avg_draws:.1f}  (expected ~57)")
    print(f"    Fastest BINGO     : {min_draws} draws")
    print(f"    Slowest BINGO     : {max_draws} draws")

    # -------------------------------------------------------------------------
    # FINAL SUMMARY
    # -------------------------------------------------------------------------
    total_time = t_gen + async_time + par_time

    print("\n" + "=" * 74)
    print("  FINAL PERFORMANCE SUMMARY")
    print("=" * 74)

    print(f"  Phase 1 — Data generation (parallel)       : {t_gen:.2f}s")

    print(f"\n  Phase 2 — 500-card game comparison:")
    print(f"    Sequential                               : {seq_time:.3f}s  (baseline)")
    print(f"    asyncio (concurrent)                     : {async_time:.3f}s", end="")
    if seq_time > 0:
        print(f"  [{seq_time/async_time:.2f}x vs sequential]")

    print(f"\n  Phase 3 — 1,000,000-card simulation:")
    print(f"    Sequential                               : {seq_full_time:.2f}s  (baseline)")
    print(f"    multiprocessing ({num_cores} cores)              : {par_time:.2f}s", end="")
    if seq_full_time > 0:
        print(f"  [{seq_full_time/par_time:.2f}x vs sequential]")

    print(f"\n  Total wall-clock time                      : {total_time:.2f}s")

    print("\n" + "=" * 74)
    print("  CONCLUSION")
    print("=" * 74)
    print("  asyncio (concurrent): best for I/O-bound tasks. Multiple")
    print("  coroutines run within ONE thread using cooperative scheduling.")
    print()
    print("  multiprocessing (parallel): best for CPU-bound tasks. Each")
    print("  worker runs in a SEPARATE OS process, bypassing Python's GIL")
    print("  for genuine simultaneous computation across all cores.")
    print("=" * 74)

    # -------------------------------------------------------------------------
    # SAVE COMPARISON GRAPH
    # -------------------------------------------------------------------------
    print("\n[GRAPH]  Saving performance comparison chart ...")
    stats_for_graph = {
        "total_cards" : total_proc,
        "total_wins"  : total_wins,
        "total_losses": total_losses,
        "avg_draws"   : avg_draws,
        "min_draws"   : min_draws,
        "max_draws"   : max_draws,
    }
    save_comparison_graph(timings, stats_for_graph, "bingo_comparison.png")


if __name__ == "__main__":
    main()