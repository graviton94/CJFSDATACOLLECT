#!/bin/bash
# Quick start script for Food Safety Intelligence Platform

set -e

echo "🍎 Food Safety Intelligence Platform - Quick Start"
echo "=================================================="
echo ""

# Check Python version
python_version=$(python3 --version 2>&1 | awk '{print $2}' | cut -d. -f1,2)
echo "✓ Python version: $(python3 --version)"

# Install dependencies
echo ""
echo "📦 Installing dependencies..."
pip install -q -r requirements.txt
echo "✓ Dependencies installed"

# Install Playwright
echo ""
echo "🎭 Installing Playwright browser..."
playwright install chromium > /dev/null 2>&1
echo "✓ Playwright browser installed"

# Create data directories
echo ""
echo "📁 Creating data directories..."
mkdir -p data/raw data/processed
echo "✓ Data directories created"

# Run data collection
echo ""
echo "🔄 Collecting data from all sources..."
python src/scheduler.py --mode once --days 7
echo "✓ Data collection complete"

# List collected files
echo ""
echo "📊 Collected data files:"
ls -lh data/processed/*.parquet 2>/dev/null || echo "No files yet"

# Show summary
echo ""
echo "📈 Data summary:"
python -c "
import pandas as pd
from pathlib import Path
import sys
sys.path.insert(0, 'src')
from utils.storage import load_all_data

df = load_all_data(Path('data/processed'))
if not df.empty:
    print(f'  Total records: {len(df)}')
    print(f'  Sources: {df[\"source\"].value_counts().to_dict()}')
    print(f'  Countries: {df[\"origin_country\"].nunique()} unique')
else:
    print('  No data collected yet')
" 2>/dev/null || echo "  Data summary not available"

echo ""
echo "=================================================="
echo "✓ Setup complete!"
echo ""
echo "Next steps:"
echo "  1. Start the dashboard: streamlit run app.py"
echo "  2. Open browser to: http://localhost:8501"
echo "  3. Run tests: python tests/test_system.py"
echo ""
echo "For scheduled collection:"
echo "  python src/scheduler.py --mode schedule --time '02:00'"
echo ""
