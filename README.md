# ARMs: Regret Minimization on Streaming Data

[![Java](https://img.shields.io/badge/Java-21%2B-orange.svg)](https://adoptium.net/)
[![Platform](https://img.shields.io/badge/platform-macOS%20%7C%20Linux%20%7C%20WSL-lightgrey.svg)](#requirements)

## Overview

A regret minimization set (RMS) is a small subset of a dataset that represents user preferences over its attributes. AS-SRMS maintains such a representative set as tuples arrive in a stream. It adapts sampled utility directions using retained history, directions derived from current representatives, and incoming data.

For each input batch, the implementation forms a candidate set from the previous representatives and the new tuples, constructs utility directions and weights, and selects up to `r` representatives using a mixed coverage-and-quality objective. The representative set and utility history are carried forward to the next batch; the full input is not retained as an in-memory candidate set.

This repository contains the updated, accelerated Java implementation and the **Colors, AQ, HALO, and Household** inputs. It does not bundle experiment results, figures, benchmark runners, or baseline implementations. The supplied launcher uses **alpha = 0.6**, **axis weight = 10**, and **seed = 42**. The current mixed-gain objective is intended for **1-RMS** (`-k 1 -rk 1`).

Input is processed in consecutive batches. A batch is a processing unit, **not an expiring sliding window**: representatives summarize the processed stream rather than only the most recent batch.

## Quick start

```bash
git clone --depth 1 https://github.com/Ptrith/ARMs.git
cd ARMs
./build.sh
./run.sh Colors 15
```

The first run decompresses the selected input into `.cache/`. Subsequent runs reuse that cache. Each run writes to a **new** directory under `result/` and prints the path to `final_representatives.txt` on completion.

Run the other included datasets with the same representative budget:

```bash
./run.sh AQ 15
./run.sh HALO 15
./run.sh Household 15
```

The launcher syntax is:

```text
./run.sh {Colors|AQ|HALO|Household} [r=15] [new_output_directory]
```

For example, `./run.sh Colors 25 result/colors_r25` selects a budget of 25 and an explicit output location. The output directory must not already exist. Run `./run.sh --help` for the syntax. Compilation is triggered automatically if the compiled entry point is missing or the source has changed.

## Requirements

- **JDK 21 or newer**, with `java` and `javac` on `PATH`, or a valid `JAVA_HOME` pointing to a JDK.
- **Bash** and **gzip**. The launch scripts target macOS, Linux, and WSL. Windows users should use WSL for the documented commands.
- **Apache Commons CLI 1.4**, included as `libs/commons-cli-1.4.jar`.
- Enough disk space for the compressed inputs and their decompressed caches. The launcher permits a maximum Java heap of **2 GB** (`-Xmx2g`).

Python, Maven, Gradle, ONNX Runtime, and Git LFS are not required to build or run this package.

## Repository layout

| Path | Purpose |
| --- | --- |
| `src/main/RunRMS_streaming.java` | Command-line entry point, input readers, checkpoints, and output. |
| `src/structures/StreamingRMS.java` | Utility sampling, scoring, representative selection, and history updates. |
| `src/structures/StreamingState.java` | Stream state, representative and utility histories, and configuration. |
| `src/structures/CurrentUtilityPool.java` | Binds each utility direction to its source and weights. |
| `src/structures/` | Supporting score tables, indexes, data structures, and optional diagnostics. |
| `src/utils/` | Vector operations and supporting utilities. |
| `libs/` | Required JAR and its third-party license and notice. |
| `dataset/` | Four compressed input datasets. |
| `build.sh` / `run.sh` | Compile the program / run one included dataset. |

`bin/`, `.cache/`, and `result/` are generated locally and ignored by Git. Precompiled classes, old result directories, and experiment figures are not part of the current source distribution.

## Included datasets

| Dataset | Compressed input | Tuples | Attributes | Batches at `w=100` |
| --- | --- | ---: | ---: | ---: |
| Colors | `dataset/Colors.txt.gz` | 68,040 | 32 | 681 |
| AQ | `dataset/AQ.txt.gz` | 382,168 | 9 | 3,822 |
| HALO | `dataset/HALO.txt.gz` | 1,113,189 | 33 | 11,132 |
| Household | `dataset/Household.txt.gz` | 2,049,280 | 7 | 20,493 |

These are the prepared numerical inputs, not raw source records. All attributes are used with the **larger-is-better** convention, and the included coordinates lie in `[0,1]`. The compressed files preserve the values and ordering of the existing prepared inputs.

For Colors, each attribute has been min–max normalized, rounded to six decimal places, and placed in a fixed random order using seed 42. This permutation defines the input stream; the launcher does **not** reshuffle the data at each run. The other inputs retain their prepared order. **HALO corresponds to the input previously named STAQS in the project.**

The AQ file contains all **382,168** online input tuples. If comparing against historical evaluations that used a 380,000-tuple reference prefix, explicitly use that same prefix in the external evaluator; the launcher does not truncate the stream.

### Text format

After decompression, each input is an ordinary whitespace-separated text file:

```text
0.120000 0.830000 0.450000 0.660000
0.920000 0.210000 0.700000 0.180000
```

- One tuple per line, with exactly `d` numeric attributes in the prepared files.
- No column names, tuple IDs, row-count header, or leading dimension line.
- Spaces and tabs are accepted; comma-separated CSV must be converted first.
- Use finite coordinates and keep the attribute dimension consistent.
- Blank lines are ignored. The parser skips short lines with a warning, reads only the first `d` fields of longer lines, and may fail on nonnumeric tokens. Validate custom inputs before running.

Internally, a tuple `x` is lifted with an additional coordinate `sqrt(d - ||x||^2)`. Nonnegative normalization to `[0,1]` satisfies the required norm bound. The program does not automatically normalize arbitrary custom data or convert smaller-is-better attributes; do this consistently before running any compared methods.

## Building

From the repository root:

```bash
./build.sh
```

This compiles the Java sources into `bin/` using UTF-8 and `--release 21`. To invoke the compiler directly:

```bash
mkdir -p bin
javac --release 21 -encoding UTF-8 \
    -cp libs/commons-cli-1.4.jar -d bin \
    src/main/*.java src/structures/*.java src/utils/*.java
```

No precompiled application binary is required. Re-run `build.sh` after editing the sources.

## Configuration

The following are the values **explicitly passed by `run.sh`**, not necessarily the defaults of a bare Java invocation. Edit the corresponding variables or flags in that script when using a different configuration.

| Setting | Java flag | Launcher value | Meaning |
| --- | --- | --- | --- |
| Representative budget | `-r` | Positional argument, default `15` | Upper bound on the output-set size. |
| Mixed-objective coefficient | `-a` | `0.6` | Weight of the quality component relative to coverage. |
| Axis objective weight | `-aw` | `10` | Weight assigned to coordinate-axis directions when objective weighting is active. |
| Algorithm seed | `-sd` | `42` | Reproducible randomized sampling and retention. |
| Batch size | `-ws` | `100` | Tuples read per batch in packed-stream mode. |
| Sampling budget | `-m` | `100` | Nominal sampling budget; deduplication and added axis directions affect actual pool size. |
| Historical retention strength | `-l` | `0.7` | Controls the age-dependent retention factor; it is not a temporal decay parameter. |
| Direction similarity threshold | `-t` | `0.985` | Used when constructing adaptive directions. |
| Diversity threshold | `-c` | `0.99` | Cosine threshold used by the relevant selection logic. |
| History / seed / new ratios | `-rh -rs -rn` | `0.6 0.2 0.2` | Sampling-mixture settings; unused capacity can be filled by new directions. |
| Sampling mode | `-sm` | `adaptive` | Adaptive direction generation. |
| Axis samples | `-ase` | `true` | Enables coordinate-axis probes. |
| Utility feedback | `-uf` | `true` | Enables feedback into utility weighting and history. |
| Objective weighting | `-ow` | `true` | Enables nonuniform weights in the current objective. |
| Selection strategy | `-p3s` | `mixed_gain` | Greedy marginal-gain selection on the complete current candidate set. |
| Index mode | `-idx` | `tree` | Enables the tree-based indexing implementation. |
| Coordinate-maximum bonus | `-wb` | `0` | Disables the optional score bonus. |
| RMS configuration | `-k -rk` | `1 1` | Use the supported 1-RMS objective. |
| Index approximation parameter | `-e` | `0.01` | Parameter passed to the indexing routines. |
| Reduced-output runtime | `-fe` | `true` | Buffers output and avoids per-batch detailed exports and quality diagnostics. |

The axis **objective weight** is separate from the fixed axis **history-retention priority** of 100. Changing `-aw` does not change that retention priority. The seed for Colors preprocessing is also separate from the algorithm seed `-sd`.

### Other supported modes

These switches remain available through the Java CLI:

- `-sm fixed`: reuse an initial stratified direction sample; reinitialize if the required sample count changes.
- `-sm uniform`: regenerate stratified samples from positions in the current candidate set. This is not uniform sampling over the utility sphere.
- `-ase false`: disable adding coordinate-axis probes. For fixed/uniform runs without axes, set both the sampling mode and this flag, and start from a fresh state.
- `-ow false`: use equal current objective weights; keep `-uf true` if only disabling objective weighting.
- `-p3s absolute_topr`: select by absolute scores instead of marginal gain.
- `-idx none`: bypass tree construction.

Use a fresh output directory for each configuration; do not reuse a checkpoint from a different configuration or input stream.

## Direct CLI usage and custom inputs

The recommended **packed-stream** mode reads one decompressed file sequentially. It is not necessary to create thousands of individual batch files. The following example assumes a normalized custom file `my_data.txt` with **10,000 tuples and four attributes**:

```bash
java -Xms256m -Xmx2g -Dfile.encoding=UTF-8 \
    -Duser.language=en -Duser.country=US \
    -cp "bin:libs/commons-cli-1.4.jar" main.RunRMS_streaming \
    -i my_data.txt -sf my_data.txt -ws 100 \
    -o result/custom_run -s result/custom_run/state.ser \
    -d 4 -n 100 -r 15 -k 1 -rk 1 -e 0.01 \
    -a 0.6 -aw 10 -sd 42 -m 100 -l 0.7 -t 0.985 -c 0.99 \
    -rh 0.6 -rs 0.2 -rn 0.2 -sm adaptive -ase true \
    -uf true -ow true -wb 0 -p3s mixed_gain -idx tree -fe true
```

Replace the file, dimension, and batch count with your own values. In packed mode, set `-n = ceil(number_of_valid_tuples / batch_size)`. The final partial batch is retained. `-i` remains required by the CLI; supply the same file path as `-sf`.

For compatibility with existing split inputs, omit `-sf` and pass a file prefix with `-i`. For batch index `t`, the reader first checks `<prefix>_batch_<t>.txt`, then `<prefix>_sliding_window_<t>.txt`. Indices start at zero, and `-n` is the total number of batches expected.

Unlike the launcher, a direct Java invocation can load an existing `-s` checkpoint and continue from its saved batch. Only do this with the same input, ordering, and configuration. The direct entry point can stop early on missing input; inspect `runtime_meta.txt` to verify completion. The launcher performs this final-batch check automatically.

## Outputs and timing

The launcher prints the new output directory after a successful run.

| File | Content |
| --- | --- |
| `final_representatives.txt` | Final representative-set block in reduced-output mode. |
| `summary.csv` | Per-batch counts, stage times, and internal diagnostics; first column is `batch`. |
| `timing.txt` | Per-batch stage and total times, in milliseconds. |
| `state.ser` | Serialized representative and utility-history state. |
| `runtime_meta.txt` | Runtime/input modes, requested batch count, and last processed batch. |
| `representatives.txt` | Detailed trace when explicitly enabled outside reduced-output mode; absent in the default run. |

The final representative file starts with:

```text
index <last_batch_index> <representative_count> <average_internal_score>
<coordinates of representative 1>
<coordinates of representative 2>
...
```

The internal score is **not MRR**. Exported coordinates contain the original `d` attributes, formatted to six decimal places; the extra lifted coordinate is not exported. Treat `r` as an upper bound, not a promise that every possible input returns exactly `r` tuples.

Timing fields have different scopes:

- `sampling_ms`, `index_ms`, `scoring_ms`, and `selection_ms` record internal stages.
- `total_ms` starts before reading a batch and ends after representative updating and any enabled internal quality diagnostics. It includes batch reading, but excludes the subsequent output writes, checkpoint saving, and JVM startup.
- End-to-end wall time also includes compilation if needed, decompression on the first run, JVM startup, and output handling. It should not be equated with the stage timings.

With `-fe true`, quality diagnostics are computed only for the last batch, final representatives/state are saved at completion, and summary/timing writes are buffered. Consequently, some diagnostic cells in earlier summary rows are blank. This is expected, not a failed run.

### MRR evaluation

The core program does **not** compute the paper's sampled maximum regret ratio over the full reference dataset. The coverage, diversity, and average-score fields are internal diagnostics and must not be reported as MRR. Evaluate the exported representative set separately using a clearly specified reference dataset and utility-direction sample, shared across compared methods. The evaluation seed is independent of the algorithm seed. No paper-result tables or external MRR evaluation scripts are bundled here.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| `release version 21 not supported` | Install JDK 21+ and check `javac -version` and `JAVA_HOME`. |
| `UnsupportedClassVersionError` | The Java runtime is older than the compiler target; use Java 21+. |
| `ClassNotFoundException` | Run `./build.sh` from the repository root and check `bin/main/RunRMS_streaming.class`. |
| `Permission denied` on a script | Run `chmod +x build.sh run.sh`, or use Bash directly. |
| `Output already exists` | Choose a new output directory. The launcher deliberately refuses overwrites and implicit checkpoint reuse. |
| Missing input or early termination | Check the input path, dimension, and `-n`; inspect `last_processed_batch` in `runtime_meta.txt`. |
| NaN values or parse errors | Validate numeric fields, remove headers, and normalize custom attributes before running. |
| Missing values in early diagnostic rows | Expected in reduced-output mode; diagnostics are computed on the final batch only. |
| A custom configuration appears ignored | Inspect the explicit flags in `run.sh`; changing a Java default does not override those flags. |

## Dependencies and licensing

Apache Commons CLI is distributed under its own license; see [`libs/LICENSE.txt`](libs/LICENSE.txt) and [`libs/NOTICE.txt`](libs/NOTICE.txt). Those files cover the dependency, not this project or its datasets. No separate project-wide license is currently included. Dataset redistribution and reuse are subject to the respective source terms.

## Contributing and contact

Bug reports and focused improvements to documentation, portability, and implementation are welcome. When reporting an issue, include the JDK version, dataset/dimension, command, and relevant error message; do not commit generated results or caches.

**Email:** [lzixu00@gmail.com](mailto:lzixu00@gmail.com)
