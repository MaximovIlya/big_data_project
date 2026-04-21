#!/bin/bash
# Post-processing: print pipeline summary and verify outputs

set -e

echo "=== Post-processing: Pipeline Summary ==="

echo ""
echo "--- Output files ---"
find output/ -type f | sort

echo ""
echo "--- Saved models ---"
find models/ -mindepth 1 -maxdepth 1 -type d | sort

echo ""
echo "--- EDA insights generated ---"
find output/eda/ -mindepth 1 -maxdepth 1 -type d | wc -l | xargs echo "Insight directories:"

echo ""
echo "--- Model metrics ---"
if [ -f output/metrics/metrics_summary.json ]; then
    cat output/metrics/metrics_summary.json
else
    echo "No metrics_summary.json found yet."
fi

echo ""
echo "=== Pipeline complete ==="
