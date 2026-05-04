#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
venv_python="$repo_root/.venv/bin/python"

cd "$repo_root"

if [[ ! -x "$venv_python" ]]; then
  python3 -m venv .venv
fi

"$venv_python" -m pip install --upgrade pip
"$venv_python" -m pip install -r requirements.txt
"$venv_python" ingestion/run_fidelity_pipeline.py

today="$(date +%F)"
cleaned_csv="$repo_root/data/$today/fidelity_funds_data_cleaned.csv"

printf "\nPipeline complete.\n"
printf "Cleaned CSV: %s\n" "$cleaned_csv"
printf "Update .env.local with:\n"
printf "FIDELITY_CSV_PATH=data/%s/fidelity_funds_data_cleaned.csv\n" "$today"
