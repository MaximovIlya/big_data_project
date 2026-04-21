#!/bin/bash
# Stage IV: Visualization — Streamlit dashboard
# Reads results from output/ and serves an interactive app

set -e

echo "=== Stage IV: Streamlit Dashboard ==="

STREAMLIT_PORT="${STREAMLIT_PORT:-8501}"

streamlit run scripts/stage4_app.py \
    --server.port "$STREAMLIT_PORT" \
    --server.headless true \
    --browser.gatherUsageStats false

echo "=== Stage IV: Dashboard running on port $STREAMLIT_PORT ==="
