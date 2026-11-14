#!/bin/sh
# Runs GcPauseBenchmark once per collector and saves the output and the JVM's own GC log.
# Usage (from the project folder, after "mvn -q compile"):   scripts/gc-pause.sh [heap] [entries] [threads]
set -e
HEAP=${1:-2g}
ENTRIES=${2:-2000000}
THREADS=${3:-4}
mkdir -p gc-results
for C in G1GC ParallelGC ZGC; do
  echo "==== $C, heap $HEAP, $ENTRIES entries"
  java -Xms$HEAP -Xmx$HEAP -XX:+Use$C "-Xlog:gc,gc+phases:file=gc-results/gc-$C.log" -cp target/classes \
    com.distri.cache.GcPauseBenchmark --entries "$ENTRIES" --seconds 60 --threads "$THREADS" | tee gc-results/run-$C.txt
  java -cp target/classes com.distri.cache.GcPauseBenchmark --summarize-gc-log gc-results/gc-$C.log | tee gc-results/pauses-$C.txt
done
