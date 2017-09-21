#!/bin/bash
# ./run_benchmarks.sh > $(date +"results/all-%Y%m%d-%H%M%S.json")

names="1.1 1.2 1.3 1.1b 1.2b 1.3b 1.1c 1.2c 1.3c 2.1 2.2 2.3 3.1 3.2 3.3 3.4 4.1 4.2 4.3"
for name in $names; do
    curl -s localhost:8000/query/$name
done
