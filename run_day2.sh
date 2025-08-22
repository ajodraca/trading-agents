#!/usr/bin/env bash
# Run daily technical analysis pipeline
# 1. Compute technical features
# 2. Generate top-100 rankings

# Exit on error, undefined vars, and pipe failures
set -euo pipefail

# Load environment variables if .env exists
if [ -f .env ]; then
    source .env
else
    echo "Error: .env file not found"
    exit 1
fi

# Check if virtual environment is activated
if [ -z "${VIRTUAL_ENV-}" ]; then
    if [ -d .venv ]; then
        echo "Activating virtual environment..."
        source .venv/bin/activate
    else
        echo "Error: Virtual environment not found. Please run setup first."
        exit 1
    fi
fi

# Function to display step information
step() {
    echo "➡️  $1"
}

# Function to display success message
success() {
    echo "✅ $1"
}

# Function to display error message and exit
error() {
    echo "❌ $1"
    exit 1
}

# Check required files exist
if [ ! -f "data/universe/sp100.csv" ]; then
    error "Universe file not found at data/universe/sp100.csv"
fi

# Step 1: Compute technical features
step "Computing technical features from 2020 to today..."
python services/features/technical_features.py \
    --start 2020-01-01 \
    --end today \
    --universe-file data/universe/sp100.csv \
    --delete-existing-window 10 || error "Failed to compute features"
success "Features computed successfully"

# Step 2: Generate rankings
step "Generating technical rankings..."
python services/agents/technical_ranker.py \
    --as-of latest \
    --universe-file data/universe/sp100.csv || error "Failed to generate rankings"
success "Rankings generated successfully"

echo "🎉 Daily technical analysis pipeline completed successfully!"
