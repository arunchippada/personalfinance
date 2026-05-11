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
data_dir="$repo_root/data/$today"
cleaned_csv="$data_dir/fidelity_funds_data_cleaned.csv"
enriched_csv="$data_dir/fidelity_funds_data_enriched.csv"

"$venv_python" ingestion/classify_and_score_funds.py "$data_dir"

printf "\nPipeline complete.\n"
printf "Cleaned CSV: %s\n" "$cleaned_csv"
printf "Enriched CSV: %s\n" "$enriched_csv"
printf "Update .env.local with:\n"
printf "FIDELITY_CSV_PATH=data/%s/fidelity_funds_data_enriched.csv\n" "$today"
