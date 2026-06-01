import fs from "fs/promises";
import path from "path";
import Papa from "papaparse";
import type {
  BaseRankField,
  Fund,
  FundType,
  Portfolio,
  PortfolioHolding,
  PortfolioMetrics,
  RankedFund,
  TopByRoleRequest,
  TopByRoleResponse,
  TopFundsByRoleResult
} from "@/lib/types";

const DEFAULT_CSV_PATH = path.join(
  process.cwd(),
  "data",
  "2026-05-10",
  "fidelity_funds_data_enriched.csv"
);

const DEFAULT_REQUEST: Required<Pick<TopByRoleRequest, "limitPerRole">> & {
  filters: NonNullable<TopByRoleRequest["filters"]>;
  rankingOptions: Required<NonNullable<TopByRoleRequest["rankingOptions"]>>;
} = {
  limitPerRole: 20,
  filters: {
    fundTypes: ["ETF", "Mutual Fund"],
    maxExpenseRatio: undefined,
    noTransactionFeeOnly: false,
    excludeLoadFunds: false
  },
  rankingOptions: {
    baseRank: "role_percentile",
    etfBonus: 0,
    noTransactionFeeBonus: 3,
    noLoadBonus: 1.5,
    lowExpenseExtraWeight: 2
  }
};

type RawCsvRow = Record<string, string>;

type FundUniverse = {
  csvPath: string;
  funds: Fund[];
  countsByFundClass: Record<string, number>;
};

type PortfolioTemplate = {
  key: Portfolio["key"];
  name: string;
  slots: Array<{ requestedRole: string; allocation: number; matchers: string[] }>;
};

const universeCache = new Map<string, Promise<FundUniverse>>();

const PORTFOLIO_TEMPLATES: PortfolioTemplate[] = [
  {
    key: "5-fund",
    name: "5-Fund Portfolio",
    slots: [
      { requestedRole: "US Large Cap Core", allocation: 35, matchers: ["US Large Cap Core"] },
      {
        requestedRole: "US Mid/Small or Extended Market",
        allocation: 10,
        matchers: ["US Mid Cap", "US Small Cap"]
      },
      {
        requestedRole: "International Developed",
        allocation: 15,
        matchers: ["International Developed Large Cap", "International Developed Small/Mid"]
      },
      { requestedRole: "Core Bond", allocation: 30, matchers: ["Core Bond"] },
      {
        requestedRole: "Short-Term Bond or TIPS",
        allocation: 10,
        matchers: ["Short-Term Bond", "Inflation-Protected Bond / TIPS"]
      }
    ]
  },
  {
    key: "7-fund",
    name: "7-Fund Portfolio",
    slots: [
      { requestedRole: "US Large Cap Core", allocation: 30, matchers: ["US Large Cap Core"] },
      { requestedRole: "US Mid Cap", allocation: 7.5, matchers: ["US Mid Cap"] },
      { requestedRole: "US Small Cap", allocation: 7.5, matchers: ["US Small Cap"] },
      {
        requestedRole: "International Developed",
        allocation: 12.5,
        matchers: ["International Developed Large Cap", "International Developed Small/Mid"]
      },
      { requestedRole: "Emerging Markets", allocation: 2.5, matchers: ["Emerging Markets Equity"] },
      { requestedRole: "Core Bond", allocation: 30, matchers: ["Core Bond"] },
      {
        requestedRole: "TIPS or Short-Term Bond",
        allocation: 10,
        matchers: ["Inflation-Protected Bond / TIPS", "Short-Term Bond"]
      }
    ]
  },
  {
    key: "9-fund",
    name: "9-Fund Portfolio",
    slots: [
      { requestedRole: "US Large Cap Core", allocation: 25, matchers: ["US Large Cap Core"] },
      {
        requestedRole: "US Large Cap Value or Growth",
        allocation: 7.5,
        matchers: ["US Large Cap Value", "US Large Cap Growth", "US Large Cap Core"]
      },
      { requestedRole: "US Mid Cap", allocation: 7.5, matchers: ["US Mid Cap"] },
      { requestedRole: "US Small Cap", allocation: 5, matchers: ["US Small Cap"] },
      {
        requestedRole: "International Developed",
        allocation: 12.5,
        matchers: ["International Developed Large Cap", "International Developed Small/Mid"]
      },
      { requestedRole: "Emerging Markets", allocation: 5, matchers: ["Emerging Markets Equity"] },
      { requestedRole: "Core Bond", allocation: 25, matchers: ["Core Bond"] },
      { requestedRole: "TIPS", allocation: 5, matchers: ["Inflation-Protected Bond / TIPS"] },
      { requestedRole: "Short-Term Bond", allocation: 5, matchers: ["Short-Term Bond"] }
    ]
  }
];

const ROLE_PRIORITY = [
  "US Large Cap Core",
  "US Large Cap Value",
  "US Large Cap Growth",
  "US Mid Cap",
  "US Small Cap",
  "International Developed Large Cap",
  "International Developed Small/Mid",
  "Emerging Markets Equity",
  "Core Bond",
  "Inflation-Protected Bond / TIPS",
  "Short-Term Bond",
  "Global / International Bond",
  "High Yield / Credit Bond",
  "Municipal Bonds",
  "Sector / Thematic Equity",
  "Real Estate / REIT",
  "Alternatives / Trading / Commodities",
  "Allocation / Target Date / Multi-Asset",
  "Other",
  "Unclassified"
];

function parseNumeric(value?: string): number | undefined {
  if (!value) return undefined;
  const cleaned = value.replace(/[$,%]/g, "").trim();
  const match = cleaned.match(/-?\d+(\.\d+)?/);
  if (!match) return undefined;
  const parsed = Number.parseFloat(match[0]);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function parseBooleanFlag(value?: string): boolean | undefined {
  if (!value) return undefined;
  const normalized = value.trim().toLowerCase();
  if (normalized === "y" || normalized === "yes" || normalized === "true") return true;
  if (normalized === "n" || normalized === "no" || normalized === "false") return false;
  return undefined;
}

function detectTicker(row: RawCsvRow, index: number) {
  const ticker = (row.Ticker ?? row.ticker ?? "").trim();
  return ticker || `FUND-${index + 1}`;
}

function detectIsEtf(row: RawCsvRow) {
  const name = (row.Name ?? "").toLowerCase();
  return /\betf\b/.test(name);
}

function normalizeFund(row: RawCsvRow, index: number): Fund {
  const ticker = detectTicker(row, index);
  const isEtf = detectIsEtf(row);

  return {
    id: `${ticker}-${index}`,
    ticker,
    symbol: ticker,
    name: (row.Name ?? "").trim(),
    fundClass: (row["Fund Class"] ?? "Unclassified").trim() || "Unclassified",
    fundClassUse: (row["Fund Class Use"] ?? "").trim() || undefined,
    morningstarCategory: (row["Morningstar Category"] ?? "").trim() || undefined,
    morningstarRisk: parseNumeric(row["Morningstar Category Risk"]),
    riskScore: parseNumeric(row.risk_score),
    tenYearRank: parseNumeric(row["10-year Rank"]),
    isEtf,
    fundType: isEtf ? "ETF" : "Mutual Fund",
    ntf: parseBooleanFlag(row.NTF),
    load: parseBooleanFlag(row["Load (Y/N)"]),
    netExpenseRatio: parseNumeric(row["Expense Ratio - Net"]),
    returns: {
      "3Y": parseNumeric(row["3 Yr"]),
      "5Y": parseNumeric(row["5 Yr"]),
      "10Y": parseNumeric(row["10 Yr"])
    },
    standardDeviation: parseNumeric(row["Standard Deviation"]),
    sharpeRatio3Y: parseNumeric(row["3 Year Sharpe Ratio"]),
    costScore: parseNumeric(row.cost_score),
    weightedReturnScore: parseNumeric(row.weighted_return_score),
    riskAdjustedScore: parseNumeric(row.risk_adjusted_score),
    volatilityScore: parseNumeric(row.volatility_score),
    globalScore: parseNumeric(row.global_score),
    globalPercentile: parseNumeric(row.global_percentile),
    rolePercentile: parseNumeric(row.role_percentile),
    raw: row
  };
}

function mergeRequest(request?: TopByRoleRequest) {
  return {
    roles: request?.roles,
    limitPerRole: request?.limitPerRole ?? DEFAULT_REQUEST.limitPerRole,
    filters: {
      fundTypes: request?.filters?.fundTypes ?? DEFAULT_REQUEST.filters.fundTypes,
      maxExpenseRatio: request?.filters?.maxExpenseRatio,
      noTransactionFeeOnly:
        request?.filters?.noTransactionFeeOnly ?? DEFAULT_REQUEST.filters.noTransactionFeeOnly,
      excludeLoadFunds:
        request?.filters?.excludeLoadFunds ?? DEFAULT_REQUEST.filters.excludeLoadFunds
    },
    rankingOptions: {
      baseRank: request?.rankingOptions?.baseRank ?? DEFAULT_REQUEST.rankingOptions.baseRank,
      etfBonus: request?.rankingOptions?.etfBonus ?? DEFAULT_REQUEST.rankingOptions.etfBonus,
      noTransactionFeeBonus:
        request?.rankingOptions?.noTransactionFeeBonus ??
        DEFAULT_REQUEST.rankingOptions.noTransactionFeeBonus,
      noLoadBonus: request?.rankingOptions?.noLoadBonus ?? DEFAULT_REQUEST.rankingOptions.noLoadBonus,
      lowExpenseExtraWeight:
        request?.rankingOptions?.lowExpenseExtraWeight ??
        DEFAULT_REQUEST.rankingOptions.lowExpenseExtraWeight
    }
  };
}

function getBaseRankValue(fund: Fund, field: BaseRankField) {
  if (field === "global_percentile") return fund.globalPercentile ?? 0;
  if (field === "global_score") return fund.globalScore ?? 0;
  return fund.rolePercentile ?? 0;
}

function round(value: number | undefined, digits = 2): number | null {
  if (value === undefined || Number.isNaN(value)) return null;
  return Number(value.toFixed(digits));
}

function compareRoles(a: string, b: string, counts?: Record<string, number>) {
  const leftPriority = ROLE_PRIORITY.indexOf(a);
  const rightPriority = ROLE_PRIORITY.indexOf(b);

  if (leftPriority !== -1 || rightPriority !== -1) {
    if (leftPriority === -1) return 1;
    if (rightPriority === -1) return -1;
    if (leftPriority !== rightPriority) return leftPriority - rightPriority;
  }

  return (counts?.[b] ?? 0) - (counts?.[a] ?? 0) || a.localeCompare(b);
}

export async function loadFunds(
  csvPath = process.env.FIDELITY_CSV_PATH ?? DEFAULT_CSV_PATH
): Promise<FundUniverse> {
  const cached = universeCache.get(csvPath);
  if (cached) return cached;

  const pending = fs
    .readFile(csvPath, "utf8")
    .then((csvText) => {
      const parsed = Papa.parse<RawCsvRow>(csvText, {
        header: true,
        skipEmptyLines: true
      });

      const funds = parsed.data
        .map((row, index) => normalizeFund(row, index))
        .filter((fund) => fund.name && fund.ticker);

      const countsByFundClass = funds.reduce<Record<string, number>>((acc, fund) => {
        acc[fund.fundClass] = (acc[fund.fundClass] ?? 0) + 1;
        return acc;
      }, {});

      return {
        csvPath,
        funds,
        countsByFundClass
      };
    })
    .catch((error) => {
      universeCache.delete(csvPath);
      throw error;
    });

  universeCache.set(csvPath, pending);
  return pending;
}

export function applyFilters(funds: Fund[], requestFilters: TopByRoleRequest["filters"] = {}) {
  const filters = {
    ...DEFAULT_REQUEST.filters,
    ...requestFilters
  };

  return funds.filter((fund) => {
    if (filters.fundTypes && filters.fundTypes.length > 0 && !filters.fundTypes.includes(fund.fundType)) {
      return false;
    }

    if (
      filters.maxExpenseRatio !== undefined &&
      fund.netExpenseRatio !== undefined &&
      fund.netExpenseRatio > filters.maxExpenseRatio
    ) {
      return false;
    }

    if (filters.noTransactionFeeOnly && fund.ntf !== true) {
      return false;
    }

    if (filters.excludeLoadFunds && fund.load === true) {
      return false;
    }

    return true;
  });
}

export function generateReasons(
  fund: Fund,
  peers: Fund[],
  rankIndex: number,
  rankingOptions: NonNullable<TopByRoleRequest["rankingOptions"]> = {}
) {
  const reasons: string[] = [];

  if (rankIndex === 0) {
    reasons.push("top-ranked in role");
  }

  const expenseValues = peers
    .map((peer) => peer.netExpenseRatio)
    .filter((value): value is number => value !== undefined)
    .sort((a, b) => a - b);
  const sharpeValues = peers
    .map((peer) => peer.sharpeRatio3Y)
    .filter((value): value is number => value !== undefined)
    .sort((a, b) => b - a);
  const longTermValues = peers
    .map((peer) => peer.returns["10Y"] ?? peer.returns["5Y"])
    .filter((value): value is number => value !== undefined)
    .sort((a, b) => b - a);

  if (
    fund.netExpenseRatio !== undefined &&
    expenseValues.length > 0 &&
    fund.netExpenseRatio <= expenseValues[Math.max(0, Math.floor(expenseValues.length * 0.25) - 1)]
  ) {
    reasons.push("very low expense ratio");
  }

  const longTermReturn = fund.returns["10Y"] ?? fund.returns["5Y"];
  if (
    longTermReturn !== undefined &&
    longTermValues.length > 0 &&
    longTermReturn >= longTermValues[Math.max(0, Math.floor(longTermValues.length * 0.25) - 1)]
  ) {
    reasons.push("strong long-term return");
  }

  if (
    fund.sharpeRatio3Y !== undefined &&
    sharpeValues.length > 0 &&
    fund.sharpeRatio3Y >= sharpeValues[Math.max(0, Math.floor(sharpeValues.length * 0.25) - 1)]
  ) {
    reasons.push("strong Sharpe ratio");
  }

  if (fund.isEtf && (rankingOptions.etfBonus ?? 0) > 0) {
    reasons.push("ETF preference applied");
  }

  if (fund.ntf && (rankingOptions.noTransactionFeeBonus ?? 0) > 0) {
    reasons.push("no-transaction-fee preference applied");
  }

  if (fund.load === false && (rankingOptions.noLoadBonus ?? 0) > 0) {
    reasons.push("no-load preference applied");
  }

  return reasons.slice(0, 5);
}

export function rerankFunds(
  funds: Fund[],
  requestRankingOptions: TopByRoleRequest["rankingOptions"] = {}
): RankedFund[] {
  const rankingOptions = {
    ...DEFAULT_REQUEST.rankingOptions,
    ...requestRankingOptions
  };

  const groupedPeers = funds.reduce<Record<string, Fund[]>>((acc, fund) => {
    (acc[fund.fundClass] ??= []).push(fund);
    return acc;
  }, {});

  const rankedByRole = Object.values(groupedPeers).flatMap((peers) => {
    const sorted = peers
      .map((fund) => {
        const baseRankValue = getBaseRankValue(fund, rankingOptions.baseRank);
        const expensePenalty = (fund.netExpenseRatio ?? 0) * rankingOptions.lowExpenseExtraWeight;
        const adjustedScore =
          baseRankValue +
          (fund.isEtf ? rankingOptions.etfBonus : 0) +
          (fund.ntf ? rankingOptions.noTransactionFeeBonus : 0) +
          (fund.load === false ? rankingOptions.noLoadBonus : 0) -
          expensePenalty;

        return {
          ...fund,
          baseRankValue,
          baseRolePercentile: fund.rolePercentile ?? null,
          adjustedScore,
          reasons: [],
          rank: 0
        } satisfies RankedFund;
      })
      .sort((a, b) => {
        if (b.adjustedScore !== a.adjustedScore) return b.adjustedScore - a.adjustedScore;
        if ((b.rolePercentile ?? -Infinity) !== (a.rolePercentile ?? -Infinity)) {
          return (b.rolePercentile ?? -Infinity) - (a.rolePercentile ?? -Infinity);
        }
        return (a.netExpenseRatio ?? Infinity) - (b.netExpenseRatio ?? Infinity);
      })
      .map((fund, index) => ({
        ...fund,
        rank: index + 1,
        reasons: generateReasons(fund, peers, index, rankingOptions)
      }));

    return sorted;
  });

  return rankedByRole;
}

export function getTopFundsByRole(
  funds: Fund[],
  request: TopByRoleRequest = {}
): {
  roles: TopFundsByRoleResult[];
  filteredFunds: Fund[];
  rankedFunds: RankedFund[];
  appliedRoles: string[];
  filteredCountsByFundClass: Record<string, number>;
} {
  const normalizedRequest = mergeRequest(request);
  const filteredFunds = applyFilters(funds, normalizedRequest.filters);
  const rankedFunds = rerankFunds(filteredFunds, normalizedRequest.rankingOptions);

  const totalByRole = funds.reduce<Record<string, number>>((acc, fund) => {
    acc[fund.fundClass] = (acc[fund.fundClass] ?? 0) + 1;
    return acc;
  }, {});

  const grouped = rankedFunds.reduce<Record<string, RankedFund[]>>((acc, fund) => {
    (acc[fund.fundClass] ??= []).push(fund);
    return acc;
  }, {});

  const filteredCountsByFundClass = filteredFunds.reduce<Record<string, number>>((acc, fund) => {
    acc[fund.fundClass] = (acc[fund.fundClass] ?? 0) + 1;
    return acc;
  }, {});

  const availableRoles = Object.keys(totalByRole).sort((a, b) => compareRoles(a, b, filteredCountsByFundClass));

  const appliedRoles = normalizedRequest.roles?.length
    ? availableRoles.filter((role) => normalizedRequest.roles?.includes(role))
    : availableRoles;

  const roles = appliedRoles.map((role) => ({
    role,
    totalInRole: totalByRole[role] ?? 0,
    totalEligibleInRole: grouped[role]?.length ?? 0,
    funds: (grouped[role] ?? []).slice(0, normalizedRequest.limitPerRole)
  }));

  return {
    roles,
    filteredFunds,
    rankedFunds,
    appliedRoles,
    filteredCountsByFundClass
  };
}

function pickHoldingForSlot(
  slot: PortfolioTemplate["slots"][number],
  groupedRankedFunds: Record<string, RankedFund[]>,
  usedFundIds: Set<string>
) {
  let matchedRole = slot.matchers.find((matcher) => (groupedRankedFunds[matcher] ?? []).length > 0);
  let warning: string | undefined;

  if (!matchedRole) {
    const fallbackRole = Object.entries(groupedRankedFunds)
      .filter(([, funds]) => funds.length > 0)
      .sort((a, b) => b[1][0].adjustedScore - a[1][0].adjustedScore)[0]?.[0];

    matchedRole = fallbackRole;
    if (fallbackRole) {
      warning = `Used closest available role "${fallbackRole}" for "${slot.requestedRole}".`;
    }
  } else if (matchedRole !== slot.requestedRole) {
    warning = `Used closest available role "${matchedRole}" for "${slot.requestedRole}".`;
  }

  if (!matchedRole) return null;

  const selectedFund =
    (groupedRankedFunds[matchedRole] ?? []).find((fund) => !usedFundIds.has(fund.id)) ??
    groupedRankedFunds[matchedRole]?.[0];

  if (!selectedFund) return null;
  usedFundIds.add(selectedFund.id);

  return {
    requestedRole: slot.requestedRole,
    matchedRole,
    allocation: slot.allocation,
    fund: selectedFund,
    warning
  } satisfies PortfolioHolding;
}

export function computePortfolioMetrics(holdings: PortfolioHolding[]): PortfolioMetrics {
  if (holdings.length === 0) {
    return {
      weightedExpenseRatio: null,
      weightedReturnScore: null,
      costScore: null,
      riskAdjustedScore: null,
      volatilityScore: null,
      portfolioScore: null
    };
  }

  const weighted = (getter: (holding: PortfolioHolding) => number | undefined) => {
    let total = 0;
    let weight = 0;

    for (const holding of holdings) {
      const value = getter(holding);
      if (value === undefined) continue;
      total += value * holding.allocation;
      weight += holding.allocation;
    }

    return weight > 0 ? total / weight : undefined;
  };

  const costScore = weighted((holding) => holding.fund.costScore);
  const weightedReturnScore = weighted((holding) => holding.fund.weightedReturnScore);
  const riskAdjustedScore = weighted((holding) => holding.fund.riskAdjustedScore);
  const volatilityScore = weighted((holding) => holding.fund.volatilityScore);

  const portfolioScore =
    costScore !== undefined &&
    weightedReturnScore !== undefined &&
    riskAdjustedScore !== undefined &&
    volatilityScore !== undefined
      ? 0.3 * costScore + 0.3 * weightedReturnScore + 0.25 * riskAdjustedScore + 0.15 * volatilityScore
      : undefined;

  return {
    weightedExpenseRatio: round(weighted((holding) => holding.fund.netExpenseRatio)),
    weightedReturnScore: round(weightedReturnScore),
    costScore: round(costScore),
    riskAdjustedScore: round(riskAdjustedScore),
    volatilityScore: round(volatilityScore),
    portfolioScore: round(portfolioScore)
  };
}

export function buildPortfolios(roleResults: TopFundsByRoleResult[]): Portfolio[] {
  const groupedRankedFunds = roleResults.reduce<Record<string, RankedFund[]>>((acc, roleResult) => {
    acc[roleResult.role] = roleResult.funds;
    return acc;
  }, {});

  return PORTFOLIO_TEMPLATES.map((template) => {
    const usedFundIds = new Set<string>();

    // Portfolio construction stays deterministic: each template fills slots in order using the
    // highest-ranked available fund from the closest matching fund class.
    const holdings: PortfolioHolding[] = template.slots.reduce<PortfolioHolding[]>((acc, slot) => {
      const holding = pickHoldingForSlot(slot, groupedRankedFunds, usedFundIds);
      if (holding) {
        acc.push(holding);
      }
      return acc;
    }, []);

    const warnings = Array.from(
      new Set(holdings.map((holding) => holding.warning).filter((warning): warning is string => Boolean(warning)))
    );

    const metrics = computePortfolioMetrics(holdings);

    return {
      key: template.key,
      name: template.name,
      holdings,
      metrics,
      warnings,
      allocationByFund: holdings.map((holding) => ({
        name: holding.fund.name,
        ticker: holding.fund.ticker,
        allocation: holding.allocation
      })),
      allocationByRole: holdings.reduce<Array<{ role: string; allocation: number }>>((acc, holding) => {
        const existing = acc.find((item) => item.role === holding.matchedRole);
        if (existing) {
          existing.allocation = round((existing.allocation + holding.allocation) as number, 1) ?? existing.allocation;
        } else {
          acc.push({ role: holding.matchedRole, allocation: holding.allocation });
        }
        return acc;
      }, []),
      componentScoreBars: [
        { label: "Cost", value: metrics.costScore },
        { label: "Return", value: metrics.weightedReturnScore },
        { label: "Risk-Adjusted", value: metrics.riskAdjustedScore },
        { label: "Volatility", value: metrics.volatilityScore },
        { label: "Overall", value: metrics.portfolioScore }
      ]
    };
  });
}

export async function buildTopByRoleResponse(request: TopByRoleRequest = {}): Promise<TopByRoleResponse> {
  const universe = await loadFunds();
  const { roles, filteredFunds, appliedRoles, filteredCountsByFundClass } = getTopFundsByRole(
    universe.funds,
    request
  );

  return {
    summary: {
      dataFile: universe.csvPath,
      totalFunds: universe.funds.length,
      totalFilteredFunds: filteredFunds.length,
      countsByFundClass: universe.countsByFundClass,
      filteredCountsByFundClass,
      availableRoles: Object.keys(universe.countsByFundClass).sort((a, b) =>
        compareRoles(a, b, universe.countsByFundClass)
      ),
      appliedRoles
    },
    roles,
    portfolios: buildPortfolios(roles)
  };
}
