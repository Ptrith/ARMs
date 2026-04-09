# AS-SRMS: Streaming Sampled Regret Minimization Set

[![Java](https://img.shields.io/badge/Java-8%2B-orange.svg)](https://adoptium.net/)
[![Platform](https://img.shields.io/badge/platform-macOS%20%7C%20Linux%20%7C%20Windows-lightgrey.svg)](#)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

tFoundException` because `bin/` is resolved relative to the current working directory.

## Abstract

In the big-data setting, concise summaries help users cope with massive datasets. **Regret minimization set (RMS)** is a standard approach to such summarization. With sensing, social networks, and related sources, however, data often arrive as a **stream**. Prior RMS work targets **static** data or **dynamic** data with explicit updates (insertions, deletions, etc.); adapting those methods to streaming can be **inefficient or inaccurate** in both theory and practice. This motivates **regret minimization on streaming data**. We study an **adaptive sampling** scheme: utility vectors (encoding preferences over attributes) are drawn from historical information and **adjusted online** as new tuples arrive. In the paper we analyze **time complexity** *O(N)*, **space complexity** *O(1)*, and **approximation ratio** *1 − 1/e* with respect to the total number of tuples *N*, and we report experiments showing **faster running time and better approximation** than baselines.

**This repository** is the companion implementation: it processes the stream in **windows** (contiguous chunks; each chunk is one file in `dataset/...`), maintains a bounded representative set, and supports the adaptive / fixed / uniform sampling modes used in the empirical study. **Users** can **compile** the Java sources and run with NBA or other splits once **`dataset/`** is populated locally, as described below.

**Keywords:** Regret minimization set, streaming data, sampling.

## Requirements

- **JDK** 8 or newer (`javac` and `java` on `PATH`)
- **Apache Commons CLI 1.4** (already in `libs/commons-cli-1.4.jar`)

Optional: add a **`LICENSE`** file in this directory if you publish the repo; the badge above links to it.

## Repository layout (code overview)

| Path | Role |
|------|------|
| **`src/main/`** | JVM entry points: `RunRMS_streaming` (CLI streaming run), `TestNBABaseline` (small NBA sanity / regret-style demo). |
| **`src/structures/`** | Core logic and indexes: `StreamingState`, `StreamingRMS`, `DualTree`, `KdTree`, `ConeTree`, `QualityMetrics`, plus supporting types (`SetCover`, `RMSInst`, `Operations`, `RankItem`, `TopKResult`, …). |
| **`src/utils/`** | Shared helpers, mainly `VectorUtil` (norms, cosine, unit vectors) and small tuple/type utilities. |
| **`libs/`** | Third-party JARs (Commons CLI). |
| **`dataset/`** | Example inputs (streaming window files). **Not in this Git remote** — create the folder locally (see `.gitignore`) to avoid GitHub size limits. |
| **`bin/`** | Created by you when compiling; holds `.class` files (safe to delete; regenerate with `javac`). |

## Input data format (window files)

The program does **not** read a single monolithic table. It reads **one text file per window**, discovered by a fixed naming pattern.

### File naming and `-i`

- **Prefix:** pass `-i <prefix>` where `<prefix>` is the full path **without** `_sliding_window_<t>.txt`.
- **Files loaded (preferred):** `<prefix>_sliding_window_0.txt`, `<prefix>_sliding_window_1.txt`, … up to `<prefix>_sliding_window_{n-1}.txt`.
- **Legacy compatibility:** if a sliding-window file is missing, the loader also checks `<prefix>_batch_<t>.txt`.
- **`-n`:** number of windows = number of consecutive indices expected (starting at `0`). If a file is missing, the run stops at that window with an error on stderr.

Example: `-i dataset/aq9_streaming/aq9` loads files such as `dataset/aq9_streaming/aq9_sliding_window_0.txt`.

### Syntax of each window file

- **Encoding:** The reader uses the **default JVM charset** for `FileReader`. For portable UTF-8 data, start the JVM with **`-Dfile.encoding=UTF-8`** if your files contain non-ASCII characters. Pure ASCII numerics are fine without it.
- **Line structure:** Each **non-empty** line is one **tuple** (one data point in ℝ<sup>d</sup> before internal embedding).
- **Delimiter:** Tokens are split by **one or more whitespace characters** (spaces/tabs; Java regex `\\s+`). No comma-separated format unless you preprocess.
- **Fields:** The first **`d` tokens** must be parsable as `double` by Java (`Double.parseDouble`). Scientific notation (e.g. `1.2e-3`) is allowed. **Extra tokens on the same line are ignored** (only the first `d` are used).
- **Short / invalid lines:** Lines with **fewer than `d` tokens** are **skipped** and a **warning** is printed to stderr; they do not abort the whole run.
- **Empty lines:** Ignored after trimming.

### Consistency with `-d`

`-d` must match the **attribute dimension** of your tuples (the number of numeric features per line). It must be **consistent across all window files** for one run.

### Internal embedding (what the program assumes)

After reading the first `d` components into a vector **x**, the code appends one coordinate so the point lies on a constructed **(d+1)-dimensional** sphere (see `RunRMS_streaming.readWindowFile`). This requires **dim − ‖x‖² ≥ 0** where ‖x‖² is the sum of squares of the **d** attributes. In practice, datasets in this project are kept in ranges where this holds (e.g. normalized or bounded tuples). If ‖x‖² > `d`, the last coordinate becomes **NaN** and behavior is undefined—**normalize or scale** your data accordingly.

### Contrast with “raw” single-file datasets

Some folders under `dataset/` contain a **raw** file with a **first line = dimension** followed by tuples (e.g. `dataset/NBA/NBA.txt`). That format is **not** read directly by `RunRMS_streaming`. You must **split** such a file into window files as above (any external tool or script), then point `-i` at the streaming prefix.

## How to compile (build)

All commands assume the **project root** (`src/`, `libs/`, `dataset/` visible).

```bash
mkdir -p bin
javac -encoding UTF-8 -cp libs/commons-cli-1.4.jar -d bin \
    src/main/*.java \
    src/structures/*.java \
    src/utils/*.java
```

- **`-encoding UTF-8`** avoids `javac` errors when sources contain UTF-8 comments or strings.
- **Clean rebuild:** `rm -rf bin && mkdir -p bin` then run `javac` again.

### Quick verification (after compile)

```bash
rm -rf result/_verify && mkdir -p result/_verify
java -cp bin:libs/commons-cli-1.4.jar main.RunRMS_streaming \
    -i dataset/aq9_streaming/aq9 \
    -o result/_verify \
    -d 9 -k 10 -r 20 -e 0.01 -n 3 \
    -l 0.7 -m 50 -c 0.99 -a 0.1
test -f result/_verify/summary.csv && head -1 result/_verify/summary.csv | grep -q '^window,' && echo "OK: summary.csv header looks correct"
```

## Command line usage

**Minimal run (AQ, 5 windows):**

```bash
mkdir -p result/demo_aq
java -cp bin:libs/commons-cli-1.4.jar main.RunRMS_streaming \
    -i dataset/aq9_streaming/aq9 \
    -o result/demo_aq \
    -d 9 -k 10 -r 20 -e 0.01 -n 5 \
    -l 0.7 -m 50 -c 0.99 -a 0.1
```

**Full AQ split** (3822 windows):

```bash
mkdir -p result/demo_aq_full
java -cp bin:libs/commons-cli-1.4.jar main.RunRMS_streaming \
    -i dataset/aq9_streaming/aq9 \
    -o result/demo_aq_full \
    -d 9 -k 10 -r 20 -e 0.01 -n 3822 \
    -l 0.7 -m 50 -c 0.99 -a 0.1
```

**Windows:** use `;` in the classpath, e.g. `java -cp "bin;libs\commons-cli-1.4.jar" main.RunRMS_streaming ...`. PowerShell may not expand `*.java` the same way as Bash—compile from **cmd**, **Git Bash**, or **WSL** if needed.

**Main flags:** `-i` prefix, `-o` output dir, `-d` dimension, `-k` top-k, `-r` representative set size, `-e` epsilon, `-n` window count. **`-a`** is α (default in code `0.4` if omitted; paper-style runs often use **`0.1`**). See `--help` output from the program for optional flags (`-l`, `-m`, `-c`, `-sm`, …).

## Output files (under `-o`)

| File | Content |
|------|---------|
| `final_representatives.txt` | Final representative set; blocks per window |
| `representatives.txt` | Verbose per-window trace |
| `summary.csv` | Per-window stats; first column `window` |
| `timing.txt` | Timing per window |
| `state.ser` | Serialized state for resume |

## Troubleshooting

| Symptom | Likely cause | What to do |
|--------|----------------|------------|
| **`ClassNotFoundException: main.RunRMS_streaming`** | Wrong directory or not compiled | `cd` to the folder that contains `bin/` and `libs/`. Run `javac` as above. Check `ls bin/main/RunRMS_streaming.class`. |
| **`Window data file missing: ...`** | Bad `-i`, wrong `-n`, or missing window files | Ensure files exist: `<prefix>_sliding_window_0.txt` … `<prefix>_sliding_window_{n-1}.txt`. Match `-n` to the count. |
| **`javac` encoding errors or garbled comments** | Source file encoding | Always use **`-encoding UTF-8`** on `javac`. Save sources as UTF-8. |
| **Wrong or garbled numeric fields** | Data file encoding vs JVM default | Run with **`java -Dfile.encoding=UTF-8 ...`** if data files are UTF-8 with non-ASCII. |
| **Windows: `javac`/`java` or glob fails** | `cmd`/PowerShell vs Bash | Use Git Bash or WSL, or list each `.java` explicitly in `javac`. |
| **NaNs or unstable results** | ‖x‖² > d or invalid data | Normalize or scale attributes so sphere embedding is valid (see Input format). |

## Contributing & contact


- **Email:** [lzixu00@gmail.com](mailto:lzixu00@gmail.com)

Pull requests (bug fixes, clearer docs, small portability fixes) are welcome; please keep changes focused and match existing code style.

---

*Optional CI badge:* after you add GitHub Actions (or similar), you can extend the header with e.g. `![build](https://github.com/USER/REPO/actions/workflows/ci.yml/badge.svg)`.
