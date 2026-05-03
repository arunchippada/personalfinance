export type FundRole =
  | "US Large Cap"
  | "US Extended Market / Mid-Small Cap"
  | "International Developed Equity"
  | "Emerging Markets Equity"
  | "Sector / Thematic Equity"
  | "Core Bond"
  | "Short-Term Bond"
  | "TIPS / Inflation-Protected Bond"
  | "Other / Excluded";

export type InferredColumns = Partial<{
  name: string;
  category: string;
  expenseNet: string;
  expenseGross: string;
  oneYear: string;
  threeYear: string;
  fiveYear: string;
  tenYear: string;
  life: string;
  ratingOverall: string;
  ratingFiveYear: string;
  riskCategory: string;
  standardDeviation: string;
  sharpeRatio: string;
  beta: string;
  assets: string;
  duration: string;
  maturity: string;
  thirtyDayYield: string;
  sevenDayYield: string;
  inceptionDate: string;
}>;

export interface FundRecord {
  id: string;
  name: string;
  ticker: string;
  role: FundRole;
  expenseRatio?: number;
  returns: Partial<Record<"1Y" | "3Y" | "5Y" | "10Y" | "Life", number>>;
  morningstarRating?: number;
  riskCategory?: number;
  standardDeviation?: number;
}

export interface ScoredFund extends FundRecord {
  score: number;
  scoreBreakdown: Array<{
    label: string;
    value: number;
    detail: string;
  }>;
  selectionReasons: string[];
}

export interface PortfolioFundSelection {
  fund: ScoredFund;
  role: FundRole;
  allocation: number;
  explanation: string[];
}

export interface WeightedMetrics {
  expenseRatio?: number;
  return1Y?: number;
  return3Y?: number;
  return5Y?: number;
  return10Y?: number;
  standardDeviation?: number;
}

export interface PortfolioRecommendation {
  key: "simple" | "balanced" | "granular";
  name: string;
  description: string;
  selectedFunds: PortfolioFundSelection[];
  stockAllocation: number;
  bondAllocation: number;
  weightedMetrics: WeightedMetrics;
  roleAllocation: Array<{ role: FundRole; allocation: number }>;
  riskSummary: string;
  tradeoffs: string[];
}

export interface BuilderConstraints {
  stockAllocation: number;
  fundTarget: number;
  maxExpenseRatio: number;
  excludeSectorThematic: boolean;
  lowCostPriority: boolean;
}

export interface PortfolioDataBundle {
  csvPath: string;
  rowCount: number;
  inferredColumns: InferredColumns;
  warnings: string[];
  funds: FundRecord[];
}
