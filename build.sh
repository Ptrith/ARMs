#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

if [[ -n "${JAVA_HOME:-}" ]]; then
    compiler="$JAVA_HOME/bin/javac"
else
    compiler=javac
fi

mkdir -p bin
"$compiler" --release 21 -encoding UTF-8 \
    -cp libs/commons-cli-1.4.jar -d bin \
    src/main/*.java src/structures/*.java src/utils/*.java
touch bin/.compiled
echo "Compiled successfully: bin/"
