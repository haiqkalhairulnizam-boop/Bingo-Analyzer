# MOHAMAD HAIQKAL BIN HAIRULNIZAM
# 🎱 Bingo Analyzer — Parallel Programming Assignment

> **ITT440 — Individual Assignment**
> Demonstrating CPU-bound parallelism and I/O-bound concurrency in Python using **1,000,000** bingo cards.


## 👤 Author

**MOHAMAD HAIQKAL BIN HAIRULNIZAM**\
Student ID: `2025221372`\
Course: ITT440 - Network Programming\
Lecturer: Shahadan Bin Saad\
Youtube Link: https://youtu.be/QVVCw44gYy8 \
Github Link: https://github.com/haiqkalhairulnizam-boop/Bingo-Analyzer.git


---

## 📌 Overview

This project simulates a large-scale bingo hall using Python's two most powerful concurrency tools:

| Technique | Module | Best for |
|---|---|---|
| **Concurrent** | `asyncio` | I/O-bound tasks, event-driven simulation |
| **Parallel** | `multiprocessing` | CPU-bound tasks, bypasses the GIL |

A sequential (single-threaded) baseline is measured first, then both approaches are benchmarked and compared. A **matplotlib performance graph** is automatically saved when the program finishes.

---

## ⚙️ System Requirements

| Component | Minimum | Recommended |
|---|---|---|
| **OS** | Windows 10 / macOS 12 / Ubuntu 20.04 | Ubuntu 22.04 / Windows 11 |
| **Python** | 3.10+ | 3.12 |
| **RAM** | 8 GB | 16 GB |
| **CPU** | 2 cores | 4+ cores (Intel i3 11th Gen or better) |
| **Storage** | 200 MB free | 500 MB free |

---

## 📦 Installation

### 1. Clone the repository

```bash
git clone https://github.com/haiqkalhairulnizam-boop/Bingo-Analyzer.git
cd Bingo-Analyzer
```

### 2. (Optional but recommended) Create a virtual environment

```bash
# Windows
python -m venv venv
venv\Scripts\activate

# macOS / Linux
python3 -m venv venv
source venv/bin/activate
```

### 3. Install dependencies

```bash
pip install matplotlib
```

> `asyncio` and `multiprocessing` are part of Python's standard library — no extra install needed.

---

## ▶️ How to Run

```bash
python bingo_analyzer.py
```

The program runs three phases automatically:

| Phase | What happens |
|---|---|
| **Phase 1** | 1,000,000 cards generated in parallel |
| **Phase 2** | Sequential vs asyncio on 500-card sample |
| **Phase 3** | Sequential vs multiprocessing on full 1M cards |
| **Output** | `bingo_comparison.png` saved to current directory |

---

## 📊 Sample Output

```
==========================================================================
  BINGO ANALYZER — Parallel Programming Assignment
  Concurrent: asyncio  |  Parallel: multiprocessing
  CPU cores detected : 4  |  Total cards : 1,000,000
==========================================================================

[PHASE 1]  Generating 1,000,000 bingo cards in parallel ...
  OK  1,000,000 unique cards ready in 3.84s

[PHASE 2]  Performance comparison — 500 cards
  Approach                                   Time        Performance
  --------------------------------------------------------------------------
    Sequential (single-threaded)             4.212s  [baseline]
    Concurrent — asyncio                     2.803s  [1.50x faster than sequential]

    Sequential game (500 cards)
      Total cards  :        500
      Wins         :        500  (100.0%)
      Losses       :          0  (0.0%)

    Asyncio game (500 cards)
      Total cards  :        500
      Wins         :        500  (100.0%)
      Losses       :          0  (0.0%)

    Async game details:
      Numbers drawn to finish : 75
      First BINGO             : Card #42 at draw #14

[PHASE 3]  Parallel simulation — 1,000,000 cards
  Workers : 4  |  Chunk size : 10,000 cards/worker
  Approach                                   Time        Performance
  --------------------------------------------------------------------------
    Sequential (1,000,000 cards)            38.500s  [baseline]
    Parallel — multiprocessing (4 cores)     9.800s  [3.93x faster than sequential]

    Parallel simulation (1,000,000 cards)
      Total cards  :  1,000,000
      Wins         :  1,000,000  (100.0%)
      Losses       :          0  (0.0%)

    Avg draws to win  : 57.1  (expected ~57)
    Fastest BINGO     : 5 draws
    Slowest BINGO     : 74 draws

==========================================================================
  FINAL PERFORMANCE SUMMARY
==========================================================================
  Phase 1 — Data generation (parallel)       : 3.84s
  Phase 2 — 500-card game comparison:
    Sequential                               : 4.212s  (baseline)
    asyncio (concurrent)                     : 2.803s  [1.50x vs sequential]
  Phase 3 — 1,000,000-card simulation:
    Sequential                               : 38.50s  (baseline)
    multiprocessing (4 cores)               :  9.80s  [3.93x vs sequential]
  Total wall-clock time                      : 16.44s
==========================================================================
```

---

## 📸 Screenshot

> ![Performance Graph](bingo_comparison.png)

The graph contains **6 panels**:

| Panel | Content |
|---|---|
| Top-left | Execution time — 500-card sample |
| Top-center | Execution time — 1,000,000 cards |
| Top-right | Speedup multipliers over sequential |
| Bottom-left | Win / Loss pie chart |
| Bottom-center | Draws-to-win statistics (fastest / avg / slowest) |
| Bottom-right | Full summary statistics table |

---

## 🗂️ Source Code Structure

```
bingo-analyzer/
├── bingo_analyzer.py        ← Main program (all sections below)
├── bingo_comparison.png     ← Auto-generated comparison graph
└── README.md
```

### Code Sections

| Section | Description |
|---|---|
| `SECTION 1` | Constants & `BingoCard` dataclass |
| `SECTION 2` | Card factory — `generate_card()` |
| `SECTION 3` | Sequential baseline — `sequential_game()` |
| `SECTION 4` | Concurrent approach — `asyncio` coroutines |
| `SECTION 5` | Parallel approach — `multiprocessing.Pool` workers |
| `SECTION 6` | Large-scale data generation (1M cards, parallel) |
| `SECTION 7` | Comparison graph — `save_comparison_graph()` |
| `SECTION 8` | Benchmark printer utilities |
| `SECTION 9` | Main orchestrator — `main()` |

---

## 🧠 Key Concepts Explained

### Why asyncio?

`asyncio` runs multiple coroutines **concurrently within a single thread** using cooperative multitasking. When a coroutine calls `await asyncio.sleep(0)`, it yields control so other coroutines can run. This is ideal for simulating the event-driven nature of a real bingo hall, where many cards are "waiting" to be checked at the same time.

```
Thread 1 (event loop)
  ├── Draw number        ← async_draw_number()
  ├── Check Card #001    ← async_check_card()  ─┐
  ├── Check Card #002    ← async_check_card()   │  gathered concurrently
  ├── ...                                        │
  └── Check Card #500    ← async_check_card()  ─┘
```

### Why multiprocessing?

Python's **Global Interpreter Lock (GIL)** prevents true thread-level CPU parallelism. `multiprocessing` sidesteps this by spawning **separate OS processes**, each with its own Python interpreter. Each process runs on its own CPU core simultaneously.

```
Core 0  → Worker 0  → Cards       0 – 9,999
Core 1  → Worker 1  → Cards  10,000 – 19,999
Core 2  → Worker 2  → Cards  20,000 – 29,999
Core 3  → Worker 3  → Cards  30,000 – 39,999
...
```

---

## 📐 Bingo Card Rules

- Standard **5×5** grid
- Columns: **B** (1–15), **I** (16–30), **N** (31–45), **G** (46–60), **O** (61–75)
- Centre square is a **FREE space** (pre-marked)
- Win conditions: any complete **row**, **column**, or **diagonal**
- 75 numbers in the draw pool — every card is guaranteed to win eventually

---


