# Fidelity Portfolio Builder

Local-only Next.js + TypeScript app for exploring a Fidelity fund universe and assembling simple model portfolios from a CSV export.

## What this does

- Loads a local Fidelity funds CSV from disk
- Detects likely columns by name instead of relying on one exact schema
- Classifies funds into deterministic portfolio roles
- Scores funds within each role using transparent heuristics
- Builds three portfolio options:
  - Simple
  - Balanced
  - Granular
- Explains why each selected fund was chosen
- Shows ranked candidates, allocations, charts, weighted fees, and weighted historical return metrics

## Expected CSV path

By default the app reads the path from `.env.local`:

```text
FIDELITY_CSV_PATH=data/2026-05-03/fidelity_funds_data_cleaned.csv
```

Use a relative path so the same setting works on Windows and macOS. If `.env.local` is missing, copy `.env.example`.

You can still override this at runtime with `FIDELITY_CSV_PATH=/absolute/path/to/file.csv`.

## How to run locally

1. Install dependencies:

```bash
pnpm install
```

2. Start the app:

On macOS/Linux:

```bash
./scripts/start_dev.sh
```

On Windows PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/windows/start_dev.ps1
```

Or, if your shell already has the needed env vars:

```bash
pnpm dev
```

3. Open the local URL shown by Next.js, typically:

`http://localhost:3000`

## Refresh Fidelity data

On macOS/Linux:

```bash
./scripts/run_fidelity_pipeline.sh
```

On Windows PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/windows/run_fidelity_pipeline.ps1
```

The script creates `.venv` if needed, installs the Python ingestion dependencies from `requirements.txt`, downloads the latest Fidelity fund data, and writes the cleaned CSV to:

```text
data/YYYY-MM-DD/fidelity_funds_data_cleaned.csv
```

To point the app at the refreshed file, update `.env.local`:

```text
FIDELITY_CSV_PATH=data/YYYY-MM-DD/fidelity_funds_data_cleaned.csv
```

## Project structure

- `app/`
  - App Router pages and global styles
- `components/portfolio-builder.tsx`
  - Main interactive UI
- `lib/fund-data.ts`
  - CSV loading
  - column inference
  - fund normalization
  - classification
  - scoring
  - portfolio generation
  - explanation generation
- `lib/types.ts`
  - shared TypeScript types

## How scoring works

Funds are scored within each portfolio role, not across the full universe.

The V1 scoring model uses a weighted combination of:

- lower net expense ratio
- stronger long-term return proxy
  - prefers `10 Yr`
  - falls back to `Life of Fund`
  - then `3 Yr`
- stronger `5 Yr` return
- lower risk
  - prefers `Standard Deviation`
  - falls back to `Morningstar Category Risk`
- higher Morningstar rating when present

Additional scoring rules:

- sector/thematic funds receive a fixed penalty
- very high expense ratios receive extra penalties
- missing values do not break the app
- if a metric is missing, scoring reweights around the available metrics

The `Low-cost priority` toggle increases the weight on expense ratio.

## Role classification

V1 uses deterministic text rules based on fund name and Morningstar category to place funds into:

- US Large Cap
- US Extended Market / Mid-Small Cap
- International Developed Equity
- Emerging Markets Equity
- Sector / Thematic Equity
- Core Bond
- Short-Term Bond
- TIPS / Inflation-Protected Bond
- Other / Excluded

These rules are intentionally easy to inspect and edit in `lib/fund-data.ts`.

## Portfolio generation

V1 does not perform optimization.

Instead, it:

- builds small role templates for Simple, Balanced, and Granular portfolios
- scales those templates to the chosen stock/bond mix
- picks the top-ranked eligible fund in each role
- uses the next-best fund when the same role appears twice in a template

This keeps the output deterministic and easy to trace.

## Known V1 limitations

- role classification is heuristic and category-based
- the app does not validate whether a fund is currently available to buy in a specific account
- weighted historical returns are descriptive, not predictive
- some bond subtypes such as high-yield or multisector bond are currently pushed into `Other / Excluded`
- Morningstar data is only used if it already exists in the CSV
- no tax, account type, turnover, or minimum-investment logic is included
- no external APIs or live market data are used
- this is a research tool and not personalized investment advice
