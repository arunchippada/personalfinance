$ErrorActionPreference = "Stop"

$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$VenvPython = Join-Path $RepoRoot ".venv\Scripts\python.exe"
$Requirements = Join-Path $RepoRoot "requirements.txt"
$Pipeline = Join-Path $RepoRoot "ingestion\run_fidelity_pipeline.py"

Set-Location $RepoRoot

function Invoke-Native {
    param(
        [string] $Description,
        [scriptblock] $Command
    )

    & $Command
    if ($LASTEXITCODE -ne 0) {
        throw "$Description failed with exit code $LASTEXITCODE."
    }
}

if (-not (Test-Path $VenvPython)) {
    $Python = Get-Command python -ErrorAction SilentlyContinue
    $PyLauncher = Get-Command py -ErrorAction SilentlyContinue

    if ($Python) {
        Invoke-Native "Creating virtual environment" { & python -m venv .venv }
    } elseif ($PyLauncher) {
        Invoke-Native "Creating virtual environment" { & py -3 -m venv .venv }
    } else {
        throw "Python was not found. Install Python 3 and rerun this script."
    }
}

Invoke-Native "Upgrading pip" { & $VenvPython -m pip install --upgrade pip }
Invoke-Native "Installing Python dependencies" { & $VenvPython -m pip install -r $Requirements }
Invoke-Native "Running Fidelity pipeline" { & $VenvPython $Pipeline }

$Today = Get-Date -Format "yyyy-MM-dd"
$CleanedCsv = Join-Path $RepoRoot "data\$Today\fidelity_funds_data_cleaned.csv"

Write-Host ""
Write-Host "Pipeline complete."
Write-Host "Cleaned CSV: $CleanedCsv"
Write-Host "Update .env.local with:"
Write-Host "FIDELITY_CSV_PATH=data/$Today/fidelity_funds_data_cleaned.csv"
