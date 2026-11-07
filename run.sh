#!/usr/bin/env bash
set -euo pipefail

usage() {
    echo "Usage: $0 {Colors|AQ|HALO|Household} [r=15] [new_output_directory]"
}
if [[ $# -eq 1 && ( "$1" == "--help" || "$1" == "-h" ) ]]; then
    usage
    exit 0
fi
if [[ $# -lt 1 || $# -gt 3 ]]; then
    usage >&2
    exit 2
fi

case "$1" in
    Colors|colors)       dataset=Colors; dim=32; rows=68040 ;;
    AQ|aq)               dataset=AQ; dim=9; rows=382168 ;;
    HALO|halo)           dataset=HALO; dim=33; rows=1113189 ;;
    Household|household) dataset=Household; dim=7; rows=2049280 ;;
    *) usage >&2; exit 2 ;;
esac
capacity="${2:-15}"
if [[ ! "$capacity" =~ ^[1-9][0-9]{0,8}$ ]]; then
    echo "r must be an integer between 1 and 999999999." >&2
    exit 2
fi

root="$(cd "$(dirname "$0")" && pwd)"
# Resolve a user-supplied relative output path before changing directories.
if [[ $# -eq 3 ]]; then
    case "$3" in
        /*) output="$3" ;;
        *) output="$PWD/$3" ;;
    esac
else
    output="$root/result/${dataset}_r${capacity}_$(date +%Y%m%d_%H%M%S)_$$"
fi
if [[ -e "$output" || -L "$output" ]]; then
    echo "Output already exists; choose a new directory: $output" >&2
    exit 2
fi
cd "$root"

needs_build=false
if [[ ! -f bin/.compiled || ! -f bin/main/RunRMS_streaming.class ]]; then
    needs_build=true
else
    for source_file in src/main/*.java src/structures/*.java src/utils/*.java libs/*.jar build.sh; do
        if [[ "$source_file" -nt bin/.compiled ]]; then needs_build=true; break; fi
    done
fi
if [[ "$needs_build" == true ]]; then ./build.sh; fi

archive="$root/dataset/$dataset.txt.gz"
input="$root/.cache/$dataset.txt"
mkdir -p .cache
if [[ ! -f "$input" || "$archive" -nt "$input" ]]; then
    echo "Decompressing $dataset input (first run only)..."
    cache_temp="$(mktemp "$root/.cache/${dataset}.XXXXXX")"
    trap 'if [[ -n "${cache_temp:-}" && -f "$cache_temp" ]]; then rm -- "$cache_temp"; fi' EXIT
    gzip -dc "$archive" > "$cache_temp"
    actual_rows="$(wc -l < "$cache_temp" | tr -d '[:space:]')"
    if [[ "$actual_rows" != "$rows" ]]; then
        echo "Unexpected row count: $actual_rows; expected $rows." >&2
        exit 1
    fi
    mv "$cache_temp" "$input"
    cache_temp=""
fi

# Defaults used by the supplied launcher; Java CLI defaults are retained separately.
alpha=0.6
axis_weight=10
seed=42
batch_size=100
samples=100
batches=$(( (rows + batch_size - 1) / batch_size ))

if [[ -n "${JAVA_HOME:-}" ]]; then
    runtime="$JAVA_HOME/bin/java"
else
    runtime=java
fi
mkdir -p "$(dirname "$output")"
mkdir "$output"
"$runtime" -Xms256m -Xmx2g -Dfile.encoding=UTF-8 -Duser.language=en -Duser.country=US \
    -cp "$root/bin:$root/libs/commons-cli-1.4.jar" main.RunRMS_streaming \
    -i "$input" -sf "$input" -ws "$batch_size" \
    -o "$output" -s "$output/state.ser" -d "$dim" -n "$batches" \
    -k 1 -rk 1 -r "$capacity" -e 0.01 -a "$alpha" -aw "$axis_weight" \
    -l 0.7 -m "$samples" -t 0.985 -c 0.99 -rh 0.6 -rs 0.2 -rn 0.2 \
    -sm adaptive -sd "$seed" -ase true -wb 0 -uf true -ow true \
    -p3s mixed_gain -idx tree -fe true

# The Java entry point can print an exception without a nonzero exit status.
# Require a complete final batch before reporting success.
if [[ ! -s "$output/final_representatives.txt" || ! -f "$output/runtime_meta.txt" ]] || \
   ! grep -qx "last_processed_batch=$((batches - 1))" "$output/runtime_meta.txt"; then
    echo "Run did not complete. Inspect the console output and $output" >&2
    exit 1
fi
echo "Completed: $dataset, r=$capacity"
echo "Representatives: $output/final_representatives.txt"
