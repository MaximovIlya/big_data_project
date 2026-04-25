#!/bin/bash
# Stage IV: Visualization — Superset export + Streamlit dashboard

set -e

echo "=== Stage IV: Superset Export ==="
python3 scripts/stage4_superset.py

echo "=== Stage IV: Streamlit Dashboard ==="

STREAMLIT_PORT="${STREAMLIT_PORT:-8501}"

# Start dashboard in background so main.sh can continue
streamlit run scripts/stage4_app.py \
    --server.port "$STREAMLIT_PORT" \
    --server.headless true \
    --browser.gatherUsageStats false &

STREAMLIT_PID=$!
echo "Streamlit started (PID=$STREAMLIT_PID) on port $STREAMLIT_PORT"

# Wait briefly to confirm it launched without errors
sleep 10

if kill -0 "$STREAMLIT_PID" 2>/dev/null; then
    echo "Dashboard is running at http://$(hostname):$STREAMLIT_PORT"
    # Leave running — stop manually with: kill $STREAMLIT_PID
else
    echo "WARNING: Streamlit exited early (check logs above)"
fi

echo "=== Stage IV complete ==="
