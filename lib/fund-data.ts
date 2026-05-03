import fs from "fs/promises";
import path from "path";
import Papa from "papaparse";
import type { FundRecord, FundRole, InferredColumns, PortfolioDataBundle } from "@/lib/types";

const DEFAULT_CSV_PATH = path.join(
  process.cwd(),
  "data",
  "2026-04-26",
  "fidelity_funds_data_cleaned.csv"
);

type RawCsvRow = Record<string, string>;

const FIELD_CANDIDATES: Record<keyof InferredColumns, string[]> = {
  name: ["name", "fund name"],
  category: ["morningstar category", "category", "asset category"],
  expenseNet: ["expense ratio net", "net expense ratio", "expense ratio - net"],
  expenseGross: ["expense ratio gross", "gross expense ratio", "expense ratio - gross"],
  oneYear: ["1 yr", "1 year", "1yr"],
  threeYear: ["3 yr", "3 year", "3yr"],
  fiveYear: ["5 yr", "5 year", "5yr"],
  tenYear: ["10 yr", "10 year", "10yr"],
  life: ["life of fund", "life"],
  ratingOverall: ["morningstar overall", "morningstar- overall", "overall rating"],
  ratingFiveYear: ["morningstar 5yrs", "morningstar- 5yrs", "5 year rating"],
  riskCategory: ["morningstar category risk", "category risk"],
  standardDeviation: ["standard deviation", "std dev"],
  sharpeRatio: ["3 year sharpe ratio", "sharpe ratio"],
  beta: ["beta"],
  assets: ["assets millions", "assets (millions)", "assets"],
  duration: ["average duration", "duration"],
  maturity: ["average maturity", "maturity"],
  thirtyDayYield: ["30-day yield (%)", "30-day yield", "30 day yield"],
  sevenDayYield: ["7-day yield (%)", "7-day yield", "7 day yield"],
  inceptionDate: ["inception date"]
};

const IMPORTANT_FIELDS: Array<keyof InferredColumns> = [
  "name",
  "category",
  "expenseNet",
  "fiveYear",
  "standardDeviation"
];

const portfolioDataCache = new Map<string, Promise<PortfolioDataBundle>>();

function normalizeHeader(value: string): string {
  return value
    .toLowerCase()
    .replace(/[%()]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function inferColumns(headers: string[]): InferredColumns {
  const normalizedHeaderMap = new Map(headers.map((header) => [normalizeHeader(header), header]));
  const inferred: InferredColumns = {};

  for (const [field, candidates] of Object.entries(FIELD_CANDIDATES) as Array<
    [keyof InferredColumns, string[]]
  >) {
    for (const candidate of candidates) {
      const direct = normalizedHeaderMap.get(normalizeHeader(candidate));
      if (direct) {
        inferred[field] = direct;
        break;
      }
    }

    if (!inferred[field]) {
      const fuzzy = headers.find((header) =>
        candidates.some((candidate) => normalizeHeader(header).includes(normalizeHeader(candidate)))
      );
      if (fuzzy) {
        inferred[field] = fuzzy;
      }
    }
  }

  return inferred;
}

function getWarnings(inferred: InferredColumns): string[] {
  const warnings: string[] = [];

  for (const field of IMPORTANT_FIELDS) {
    if (!inferred[field]) {
      warnings.push(`Missing likely column for ${field}. Some scoring and summaries will be partial.`);
    }
  }

  return warnings;
}

function parseNumeric(value?: string): number | undefined {
  if (!value) return undefined;
  const cleaned = value.replace(/[$,%]/g, "").trim();
  const match = cleaned.match(/-?\d+(\.\d+)?/);
  if (!match) return undefined;
  const parsed = Number.parseFloat(match[0]);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function extractTicker(name: string): string {
  const match = name.match(/\(([A-Z0-9.]+)\)\s*$/);
  return match?.[1] ?? "N/A";
}

// Fund classification is a deterministic rule set based on category and name keywords.
function classifyFundRole(category: string, name: string): FundRole {
  const text = `${category} ${name}`.toLowerCase();

  if (/(inflation|tips|inflation protected)/.test(text)) {
    return "TIPS / Inflation-Protected Bond";
  }

  if (/(short term bond|ultrashort|ultra short|short duration|limited term)/.test(text)) {
    return "Short-Term Bond";
  }

  if (/(intermediate core|core bond|core plus|short government|intermediate government|long government|intermediate term bond)/.test(text)) {
    return "Core Bond";
  }

  if (/(large blend|large growth|large value|large cap|s&p 500|500 index|total market)/.test(text)) {
    return "US Large Cap";
  }

  if (/(mid cap|small cap|small blend|small growth|small value|micro cap|extended market|smid)/.test(text)) {
    return "US Extended Market / Mid-Small Cap";
  }

  if (/(diversified emerging mkts|emerging markets equity|emerging market stock|china region|latin america stock|pacific asia ex japan)/.test(text)) {
    return "Emerging Markets Equity";
  }

  if (
    /(foreign large|foreign small|world stock|world large|europe stock|japan stock|pacific|foreign|international)/.test(text) &&
    !/emerging/.test(text)
  ) {
    return "International Developed Equity";
  }

  if (
    /(technology|health|industrials|communications|consumer cyclical|financial|real estate|energy|natural resources|utilities|precious metals|infrastructure|thematic|semiconductor|biotech)/.test(
      text
    )
  ) {
    return "Sector / Thematic Equity";
  }

  return "Other / Excluded";
}

function normalizeFund(row: RawCsvRow, inferred: InferredColumns, index: number): FundRecord | null {
  const name = row[inferred.name ?? ""]?.trim();
  const category = row[inferred.category ?? ""]?.trim() ?? "";

  if (!name) return null;

  const role = classifyFundRole(category, name);

  return {
    id: `${extractTicker(name)}-${index}`,
    name: name.replace(/\s*\([A-Z0-9.]+\)\s*$/, "").trim(),
    ticker: extractTicker(name),
    role,
    expenseRatio: parseNumeric(row[inferred.expenseNet ?? ""]),
    returns: {
      "1Y": parseNumeric(row[inferred.oneYear ?? ""]),
      "3Y": parseNumeric(row[inferred.threeYear ?? ""]),
      "5Y": parseNumeric(row[inferred.fiveYear ?? ""]),
      "10Y": parseNumeric(row[inferred.tenYear ?? ""]),
      Life: parseNumeric(row[inferred.life ?? ""])
    },
    morningstarRating:
      parseNumeric(row[inferred.ratingFiveYear ?? ""]) ?? parseNumeric(row[inferred.ratingOverall ?? ""]),
    riskCategory: parseNumeric(row[inferred.riskCategory ?? ""]),
    standardDeviation: parseNumeric(row[inferred.standardDeviation ?? ""])
  };
}

async function loadPortfolioDataUncached(csvPath: string) {
  const csvText = await fs.readFile(csvPath, "utf8");
  const parsed = Papa.parse<RawCsvRow>(csvText, {
    header: true,
    skipEmptyLines: true
  });

  const headers = parsed.meta.fields ?? [];
  const inferredColumns = inferColumns(headers);
  const warnings = getWarnings(inferredColumns);
  const funds = parsed.data
    .map((row, index) => normalizeFund(row, inferredColumns, index))
    .filter((fund): fund is FundRecord => Boolean(fund));

  return {
    csvPath,
    rowCount: funds.length,
    inferredColumns,
    warnings,
    funds
  } satisfies PortfolioDataBundle;
}

// CSV loading is cached by path so dev reloads do not keep reparsing the same local file.
export function loadPortfolioData(csvPath = process.env.FIDELITY_CSV_PATH ?? DEFAULT_CSV_PATH) {
  const cached = portfolioDataCache.get(csvPath);
  if (cached) {
    return cached;
  }

  const pending = loadPortfolioDataUncached(csvPath).catch((error) => {
    portfolioDataCache.delete(csvPath);
    throw error;
  });

  portfolioDataCache.set(csvPath, pending);
  return pending;
}
