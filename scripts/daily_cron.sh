#!/bin/bash
# Daily USDVND rate update and Excel regeneration
# Run at 7 PM Vietnam time (12:00 UTC) daily

set -e

PROJECT_DIR="$HOME/Code/workspace/usdvnd-tracker"
VENV="$PROJECT_DIR/venv"
LOG_FILE="$PROJECT_DIR/data/daily_update.log"

cd "$PROJECT_DIR"
source "$VENV/bin/activate"

echo "========================================" >> "$LOG_FILE"
echo "Daily Update: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"

# Run daily update
python scrapers/daily_update.py >> "$LOG_FILE" 2>&1

# Regenerate Excel
python analysis/generate_excel.py >> "$LOG_FILE" 2>&1

# Git commit and push
cd "$PROJECT_DIR"
if [ -d ".git" ]; then
    git add -A
    git commit -m "Daily update: $(date '+%Y-%m-%d')" || true
    git push origin main || true
fi

echo "Update complete: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"
echo "" >> "$LOG_FILE"
