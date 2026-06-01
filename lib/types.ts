export type FundType = "ETF" | "Mutual Fund";

export type BaseRankField = "role_percentile" | "global_percentile" | "global_score";

export interface Fund {
  id: string;
  ticker: string;
  symbol: string;
  name: string;
  fundClass: string;
  fundClassUse?: string;
  morningstarCategory?: string;
  morningstarRisk?: number;
  riskScore?: number;
  tenYearRank?: number;
  isEtf: boolean;
  fundType: FundType;
  ntf?: boolean;
  load?: boolean;
  netExpenseRatio?: number;
  returns: Partial<Record<"3Y" | "5Y" | "10Y", number>>;
  standardDeviation?: number;
  sharpeRatio3Y?: number;
  costScore?: number;
  weightedReturnScore?: number;
  riskAdjustedScore?: number;
  volatilityScore?: number;
  globalScore?: number;
  globalPercentile?: number;
  rolePercentile?: number;
  raw: Record<string, string>;
}

export interface TopByRoleRequest {
  roles?: string[];
  limitPerRole?: number;
  filters?: {
    fundTypes?: FundType[];
    maxExpenseRatio?: number;
    noTransactionFeeOnly?: boolean;
    excludeLoadFunds?: boolean;
  };
  rankingOptions?: {
    baseRank?: BaseRankField;
    etfBonus?: number;
    noTransactionFeeBonus?: number;
    noLoadBonus?: number;
    lowExpenseExtraWeight?: number;
  };
}

export interface RankedFund extends Fund {
  baseRankValue: number;
  baseRolePercentile: number | null;
  adjustedScore: number;
  reasons: string[];
  rank: number;
}

export interface TopFundsByRoleResult {
  role: string;
  totalInRole: number;
  totalEligibleInRole: number;
  funds: RankedFund[];
}

export interface PortfolioHolding {
  requestedRole: string;
  matchedRole: string;
  allocation: number;
  fund: RankedFund;
  warning?: string;
}

export interface PortfolioMetrics {
  weightedExpenseRatio: number | null;
  weightedReturnScore: number | null;
  costScore: number | null;
  riskAdjustedScore: number | null;
  volatilityScore: number | null;
  portfolioScore: number | null;
}

export interface Portfolio {
  key: "5-fund" | "7-fund" | "9-fund";
  name: string;
  holdings: PortfolioHolding[];
  metrics: PortfolioMetrics;
  warnings: string[];
  allocationByFund: Array<{ name: string; ticker: string; allocation: number }>;
  allocationByRole: Array<{ role: string; allocation: number }>;
  componentScoreBars: Array<{ label: string; value: number | null }>;
}

export interface FundsSummary {
  dataFile: string;
  totalFunds: number;
  totalFilteredFunds: number;
  countsByFundClass: Record<string, number>;
  filteredCountsByFundClass: Record<string, number>;
  availableRoles: string[];
  appliedRoles: string[];
}

export interface TopByRoleResponse {
  summary: FundsSummary;
  roles: TopFundsByRoleResult[];
  portfolios: Portfolio[];
}
