#!/usr/bin/env bash


echo "==============Start Orchestration Process=============="

VENV_PATH="/home/scsarwa/monitoring/venv/bin/activate"

source "$VENV_PATH"

PYTHON_SCRIPT="/home/scsarwa/monitoring/elt_pipeline.py"

python3 "$PYTHON_SCRIPT"

echo "==============End Orchestration Process=============="

