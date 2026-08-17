"use client";

import { startTransition, useDeferredValue, useEffect, useMemo, useState } from "react";
import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  Cell,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis
} from "recharts";
import type {
  BaseRankField,
  FundType,
  Portfolio,
  RankedFund,
  TopByRoleRequest,
  TopByRoleResponse,
  TopFundsByRoleResult
} from "@/lib/types";

type FundTypeMode = "both" | FundType;
type AppView = "recommendations" | "compare" | "portfolio";
type SortKey =
  | "rank"
  | "ticker"
  | "name"
  | "rolePercentile"
  | "globalPercentile"
  | "globalScore"
  | "adjustedScore"
  | "expenseRatio"
  | "return3Y"
  | "return5Y"
  | "return10Y"
  | "standardDeviation"
  | "ntf"
  | "load"
  | "morningstarRisk"
  | "costScore"
  | "weightedReturnScore"
  | "riskAdjustedScore"
  | "volatilityScore";

const ROLE_COLORS = [
  "#0f766e",
  "#1d4ed8",
  "#b45309",
  "#7c3aed",
  "#0f172a",
  "#be123c",
  "#0891b2",
  "#65a30d",
  "#ea580c"
];

const DEFAULT_CONTROLS = {
  limitPerRole: 20,
  fundTypeMode: "both" as FundTypeMode,
  maxExpenseRatio: "",
  etfBonus: 0,
  noTransactionFeeBonus: 3,
  noLoadBonus: 1.5,
  lowExpenseExtraWeight: 2,
  baseRank: "global_score" as BaseRankField
};

type RankingControls = Pick<
  typeof DEFAULT_CONTROLS,
  "baseRank" | "etfBonus" | "noTransactionFeeBonus" | "noLoadBonus" | "lowExpenseExtraWeight"
>;

function formatPercent(value: number | null | undefined, digits = 2) {
  return value === null || value === undefined ? "N/A" : `${value.toFixed(digits)}%`;
}

function formatScore(value: number | null | undefined, digits = 2) {
  return value === null || value === undefined ? "N/A" : value.toFixed(digits);
}

function formatFlag(value: boolean | undefined) {
  if (value === true) return "Yes";
  if (value === false) return "No";
  return "N/A";
}

function formatFundsDataSource(dataFile: string | undefined) {
  if (!dataFile) return "Loading fund data";

  const normalizedPath = dataFile.replace(/\\/g, "/");
  const fileName = normalizedPath.split("/").pop() ?? "";
  const dateMatch = normalizedPath.match(/(20\d{2})-(\d{2})-(\d{2})/);
  const providerMatch = fileName.match(/(?:^|[_-])(fidelity|vanguard|schwab|morningstar)(?:[_-]|\.)/i);
  const provider = providerMatch
    ? providerMatch[1].charAt(0).toUpperCase() + providerMatch[1].slice(1).toLowerCase()
    : "source file";
  const updated = dateMatch
    ? new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric", year: "numeric", timeZone: "UTC" }).format(
        new Date(`${dateMatch[1]}-${dateMatch[2]}-${dateMatch[3]}T00:00:00Z`)
      )
    : null;

  return `Funds data from ${provider}${updated ? ` · Updated ${updated}` : ""}`;
}

function getRoleColor(index: number) {
  return ROLE_COLORS[index % ROLE_COLORS.length];
}

function toRequest(controls: typeof DEFAULT_CONTROLS): TopByRoleRequest {
  const fundTypes =
    controls.fundTypeMode === "both"
      ? (["ETF", "Mutual Fund"] as FundType[])
      : ([controls.fundTypeMode] as FundType[]);

  return {
    limitPerRole: controls.limitPerRole,
    filters: {
      fundTypes,
      maxExpenseRatio: controls.maxExpenseRatio === "" ? undefined : Number(controls.maxExpenseRatio)
    },
    rankingOptions: {
      baseRank: controls.baseRank,
      etfBonus: controls.etfBonus,
      noTransactionFeeBonus: controls.noTransactionFeeBonus,
      noLoadBonus: controls.noLoadBonus,
      lowExpenseExtraWeight: controls.lowExpenseExtraWeight
    }
  };
}

function metricValue(fund: RankedFund, key: SortKey) {
  switch (key) {
    case "rank":
      return fund.rank;
    case "ticker":
      return fund.ticker;
    case "name":
      return fund.name;
    case "rolePercentile":
      return fund.baseRolePercentile ?? -Infinity;
    case "globalPercentile":
      return fund.globalPercentile ?? -Infinity;
    case "globalScore":
      return fund.globalScore ?? -Infinity;
    case "adjustedScore":
      return fund.adjustedScore;
    case "expenseRatio":
      return fund.netExpenseRatio ?? Infinity;
    case "return3Y":
      return fund.returns["3Y"] ?? -Infinity;
    case "return5Y":
      return fund.returns["5Y"] ?? -Infinity;
    case "return10Y":
      return fund.returns["10Y"] ?? -Infinity;
    case "standardDeviation":
      return fund.standardDeviation ?? Infinity;
    case "ntf":
      return fund.ntf ? 1 : 0;
    case "load":
      return fund.load ? 1 : 0;
    case "morningstarRisk":
      return fund.morningstarRisk ?? Infinity;
    case "costScore":
      return fund.costScore ?? -Infinity;
    case "weightedReturnScore":
      return fund.weightedReturnScore ?? -Infinity;
    case "riskAdjustedScore":
      return fund.riskAdjustedScore ?? -Infinity;
    case "volatilityScore":
      return fund.volatilityScore ?? -Infinity;
  }
}

function sortFunds(funds: RankedFund[], sortKey: SortKey, direction: "asc" | "desc") {
  return [...funds].sort((a, b) => {
    const left = metricValue(a, sortKey);
    const right = metricValue(b, sortKey);

    if (typeof left === "string" && typeof right === "string") {
      return direction === "asc" ? left.localeCompare(right) : right.localeCompare(left);
    }

    const leftNumber = typeof left === "number" ? left : 0;
    const rightNumber = typeof right === "number" ? right : 0;
    return direction === "asc" ? leftNumber - rightNumber : rightNumber - leftNumber;
  });
}

function getMaxAdjustedScore(funds: RankedFund[]) {
  if (funds.length === 0) return null;
  return funds.reduce((max, fund) => Math.max(max, fund.adjustedScore), -Infinity);
}

function getBaseRankLabel(baseRank: BaseRankField) {
  if (baseRank === "global_percentile") return "Global %";
  if (baseRank === "global_score") return "Global score";
  return "Role %";
}

function getAdjustedScoreTooltip(fund: RankedFund, rankingControls: RankingControls) {
  const parts = [`${getBaseRankLabel(rankingControls.baseRank)} ${formatScore(fund.baseRankValue)}`];
  const etfContribution = fund.isEtf ? rankingControls.etfBonus : 0;
  const ntfContribution = fund.ntf ? rankingControls.noTransactionFeeBonus : 0;
  const noLoadContribution = fund.load === false ? rankingControls.noLoadBonus : 0;
  parts.push(`ETF +${formatScore(etfContribution)}`);
  parts.push(`NTF +${formatScore(ntfContribution)}`);
  parts.push(`No-load +${formatScore(noLoadContribution)}`);

  const expensePenalty = (fund.netExpenseRatio ?? 0) * rankingControls.lowExpenseExtraWeight;
  parts.push(
    `Expense -${formatScore(expensePenalty)} (${formatPercent(
      fund.netExpenseRatio,
      2
    )} x ${formatScore(rankingControls.lowExpenseExtraWeight)})`
  );

  parts.push(`Adjusted ${formatScore(fund.adjustedScore)}`);
  return parts.join(" | ");
}

function SortButton({
  label,
  sortKey,
  activeKey,
  direction,
  onChange,
  className
}: {
  label: string;
  sortKey: SortKey;
  activeKey: SortKey;
  direction: "asc" | "desc";
  onChange: (key: SortKey) => void;
  className?: string;
}) {
  const active = sortKey === activeKey;

  return (
    <button
      className={`inline-flex items-center gap-1 text-left text-xs uppercase tracking-[0.12em] text-slate-500 ${className ?? ""}`}
      onClick={() => onChange(sortKey)}
      type="button"
    >
      <span>{label}</span>
      <span className={active ? "text-slate-900" : "text-slate-300"}>{active ? (direction === "asc" ? "↑" : "↓") : "↕"}</span>
    </button>
  );
}

function RoleTable({
  roleResult,
  sortKey,
  direction,
  onSortChange,
  rankingControls,
  selectedFundIds,
  onToggleCompare,
  onQuickView
}: {
  roleResult: TopFundsByRoleResult;
  sortKey: SortKey;
  direction: "asc" | "desc";
  onSortChange: (key: SortKey) => void;
  rankingControls: RankingControls;
  selectedFundIds: string[];
  onToggleCompare: (fund: RankedFund) => void;
  onQuickView: (fund: RankedFund) => void;
}) {
  const sortedFunds = useMemo(
    () => sortFunds(roleResult.funds, sortKey, direction),
    [direction, roleResult.funds, sortKey]
  );

  return (
    <div className="rounded-[28px] border border-slate-200/80 bg-white/80 p-4 shadow-[0_20px_40px_rgba(15,23,42,0.06)]">
      <div className="flex flex-col gap-3 md:flex-row md:items-end md:justify-between">
        <div>
          <h2 className="text-2xl font-semibold text-slate-950">{roleResult.role}</h2>
          <p className="mt-2 text-sm text-slate-600">
            Showing {roleResult.funds.length} of {roleResult.totalEligibleInRole} eligible funds in this class.
          </p>
        </div>
        <div className="rounded-full bg-slate-100 px-3 py-2 text-xs font-semibold text-slate-700">
          {roleResult.totalInRole} total in universe
        </div>
      </div>

      <div
        className={`mt-4 overflow-auto ${sortedFunds.length > 5 ? "max-h-[26rem]" : ""}`}
      >
        <table className="min-w-[1360px] text-sm">
          <thead className="sticky top-0 bg-white/95 backdrop-blur-sm">
            <tr className="border-b border-slate-200 text-left">
              <th className="pb-3 pr-4 text-xs uppercase tracking-[0.12em] text-slate-500">Compare</th>
              <th className="pb-3 pr-4"><SortButton label="Rank" sortKey="rank" activeKey={sortKey} direction={direction} onChange={onSortChange} /></th>
              <th className="pb-3 pr-4"><SortButton label="Ticker" sortKey="ticker" activeKey={sortKey} direction={direction} onChange={onSortChange} /></th>
              <th className="pb-3 pr-4"><SortButton label="Name" sortKey="name" activeKey={sortKey} direction={direction} onChange={onSortChange} /></th>
              <th className="pb-3 pr-4"><SortButton label="Role %" sortKey="rolePercentile" activeKey={sortKey} direction={direction} onChange={onSortChange} /></th>
              <th className="pb-3 pr-4"><SortButton label="Global %" sortKey="globalPercentile" activeKey={sortKey} direction={direction} onChange={onSortChange} /></th>
              <th className="pb-3 pr-4"><SortButton label="Global score" sortKey="globalScore" activeKey={sortKey} direction={direction} onChange={onSortChange} /></th>
              <th className="pb-3 pr-4"><SortButton label="Adjusted" sortKey="adjustedScore" activeKey={sortKey} direction={direction} onChange={onSortChange} /></th>
              <th className="pb-3 pr-4"><SortButton label="Expense" sortKey="expenseRatio" activeKey={sortKey} direction={direction} onChange={onSortChange} /></th>
              <th className="pb-3 pr-4"><SortButton label="3Y" sortKey="return3Y" activeKey={sortKey} direction={direction} onChange={onSortChange} /></th>
              <th className="pb-3 pr-4"><SortButton label="5Y" sortKey="return5Y" activeKey={sortKey} direction={direction} onChange={onSortChange} /></th>
              <th className="pb-3 pr-4"><SortButton label="10Y" sortKey="return10Y" activeKey={sortKey} direction={direction} onChange={onSortChange} /></th>
              <th className="pb-3 pr-4"><SortButton label="Std Dev" sortKey="standardDeviation" activeKey={sortKey} direction={direction} onChange={onSortChange} /></th>
              <th className="pb-3 pr-4"><SortButton label="NTF" sortKey="ntf" activeKey={sortKey} direction={direction} onChange={onSortChange} /></th>
              <th className="pb-3 pr-4"><SortButton label="Load" sortKey="load" activeKey={sortKey} direction={direction} onChange={onSortChange} /></th>
              <th className="pb-3 pr-4"><SortButton label="Mstar Risk" sortKey="morningstarRisk" activeKey={sortKey} direction={direction} onChange={onSortChange} /></th>
              <th className="pb-3 pr-4"><SortButton label="Cost" sortKey="costScore" activeKey={sortKey} direction={direction} onChange={onSortChange} /></th>
              <th className="pb-3 pr-4"><SortButton label="Return Score" sortKey="weightedReturnScore" activeKey={sortKey} direction={direction} onChange={onSortChange} /></th>
              <th className="pb-3 pr-4"><SortButton label="Risk Adj" sortKey="riskAdjustedScore" activeKey={sortKey} direction={direction} onChange={onSortChange} /></th>
              <th className="pb-3 pr-4"><SortButton label="Volatility" sortKey="volatilityScore" activeKey={sortKey} direction={direction} onChange={onSortChange} /></th>
            </tr>
          </thead>
          <tbody>
            {sortedFunds.map((fund) => (
              <tr key={fund.id} className="border-b border-slate-100 align-top last:border-b-0">
                <td className="py-3 pr-4">
                  <input
                    aria-label={`Compare ${fund.ticker}`}
                    checked={selectedFundIds.includes(fund.id)}
                    className="h-4 w-4 accent-[#176B5B]"
                    onChange={() => onToggleCompare(fund)}
                    type="checkbox"
                  />
                </td>
                <td className="py-3 pr-4 font-semibold text-slate-900">{fund.rank}</td>
                <td className="py-3 pr-4 font-semibold">
                  <button
                    aria-label={`Quick view ${fund.ticker}`}
                    className="text-[#176B5B] underline decoration-[#176B5B]/30 underline-offset-4 transition hover:decoration-[#176B5B] focus-visible:rounded focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[#176B5B]"
                    onClick={() => onQuickView(fund)}
                    title={`Quick view ${fund.ticker}`}
                    type="button"
                  >
                    {fund.ticker}
                  </button>
                </td>
                <td className="py-3 pr-4 min-w-[280px] text-slate-700">{fund.name}</td>
                <td className="py-3 pr-4">{formatScore(fund.baseRolePercentile)}</td>
                <td className="py-3 pr-4">{formatScore(fund.globalPercentile)}</td>
                <td className="py-3 pr-4">{formatScore(fund.globalScore)}</td>
                <td
                  className="py-3 pr-4 font-semibold text-slate-900"
                  title={getAdjustedScoreTooltip(fund, rankingControls)}
                >
                  {formatScore(fund.adjustedScore)}
                </td>
                <td className="py-3 pr-4">{formatPercent(fund.netExpenseRatio)}</td>
                <td className="py-3 pr-4">{formatPercent(fund.returns["3Y"])}</td>
                <td className="py-3 pr-4">{formatPercent(fund.returns["5Y"])}</td>
                <td className="py-3 pr-4">{formatPercent(fund.returns["10Y"])}</td>
                <td className="py-3 pr-4">{formatPercent(fund.standardDeviation)}</td>
                <td className="py-3 pr-4">{formatFlag(fund.ntf)}</td>
                <td className="py-3 pr-4">{formatFlag(fund.load)}</td>
                <td className="py-3 pr-4">{formatScore(fund.morningstarRisk)}</td>
                <td className="py-3 pr-4">{formatScore(fund.costScore)}</td>
                <td className="py-3 pr-4">{formatScore(fund.weightedReturnScore)}</td>
                <td className="py-3 pr-4">{formatScore(fund.riskAdjustedScore)}</td>
                <td className="py-3 pr-4">{formatScore(fund.volatilityScore)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function PortfolioCard({
  portfolio,
  selected,
  onSelect
}: {
  portfolio: Portfolio;
  selected: boolean;
  onSelect: () => void;
}) {
  return (
    <button
      className={`rounded-[28px] border p-5 text-left transition ${
        selected
          ? "border-slate-900 bg-slate-900 text-white shadow-[0_24px_60px_rgba(15,23,42,0.22)]"
          : "border-slate-200 bg-white/84 text-slate-900 hover:-translate-y-0.5"
      }`}
      onClick={onSelect}
      type="button"
    >
      <div className="flex items-start justify-between gap-3">
        <div>
          <div className={`section-title ${selected ? "text-slate-300" : ""}`}>Portfolio Template</div>
          <h3 className="mt-2 text-xl font-semibold">{portfolio.name}</h3>
        </div>
        <div className={`rounded-full px-3 py-1 text-xs font-semibold ${selected ? "bg-white/20" : "bg-slate-100"}`}>
          {portfolio.holdings.length} holdings
        </div>
      </div>

      <div className="mt-5 grid grid-cols-2 gap-3">
        <div className={`rounded-2xl px-3 py-3 ${selected ? "bg-white/10" : "bg-slate-50"}`}>
          <div className={`text-[11px] uppercase tracking-[0.14em] ${selected ? "text-slate-300" : "text-slate-500"}`}>Portfolio Score</div>
          <div className="mt-1 text-lg font-semibold">{formatScore(portfolio.metrics.portfolioScore)}</div>
        </div>
        <div className={`rounded-2xl px-3 py-3 ${selected ? "bg-white/10" : "bg-slate-50"}`}>
          <div className={`text-[11px] uppercase tracking-[0.14em] ${selected ? "text-slate-300" : "text-slate-500"}`}>Weighted Expense</div>
          <div className="mt-1 text-lg font-semibold">{formatPercent(portfolio.metrics.weightedExpenseRatio)}</div>
        </div>
      </div>
    </button>
  );
}

export function PortfolioBuilder() {
  const [appView, setAppView] = useState<AppView>("recommendations");
  const [controls, setControls] = useState(DEFAULT_CONTROLS);
  const deferredControls = useDeferredValue(controls);
  const [data, setData] = useState<TopByRoleResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedRole, setSelectedRole] = useState<string | null>(null);
  const [selectedPortfolioKey, setSelectedPortfolioKey] = useState<Portfolio["key"]>("5-fund");
  const [sortKey, setSortKey] = useState<SortKey>("adjustedScore");
  const [sortDirection, setSortDirection] = useState<"asc" | "desc">("desc");
  const [quickViewFund, setQuickViewFund] = useState<RankedFund | null>(null);
  const [compareFunds, setCompareFunds] = useState<RankedFund[]>([]);
  const [portfolioOverrides, setPortfolioOverrides] = useState<Record<string, Record<string, string>>>({});

  const requestBody = useMemo(() => toRequest(deferredControls), [deferredControls]);

  useEffect(() => {
    const controller = new AbortController();

    async function fetchFunds() {
      setLoading(true);
      setError(null);

      try {
        const response = await fetch("/api/funds/top-by-role", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(requestBody),
          signal: controller.signal
        });

        const payload = await response.json();
        if (!response.ok) {
          throw new Error(payload.error ?? "Request failed.");
        }

        setData(payload as TopByRoleResponse);
      } catch (fetchError) {
        if (controller.signal.aborted) return;
        setError(fetchError instanceof Error ? fetchError.message : "Unable to load funds.");
        setData(null);
      } finally {
        if (!controller.signal.aborted) {
          setLoading(false);
        }
      }
    }

    fetchFunds();
    return () => controller.abort();
  }, [requestBody]);

  useEffect(() => {
    if (!data) return;
    const available = data.roles.map((role) => role.role);
    if (!selectedRole || !available.includes(selectedRole)) {
      setSelectedRole(available[0] ?? null);
    }
  }, [data, selectedRole]);

  useEffect(() => {
    if (!data) return;
    const availableKeys = data.portfolios.map((portfolio) => portfolio.key);
    if (!availableKeys.includes(selectedPortfolioKey)) {
      setSelectedPortfolioKey(data.portfolios[0]?.key ?? "5-fund");
    }
  }, [data, selectedPortfolioKey]);

  const activePortfolio =
    data?.portfolios.find((portfolio) => portfolio.key === selectedPortfolioKey) ?? data?.portfolios[0] ?? null;
  const sortedRoles = useMemo(() => {
    if (!data) return [];

    return [...data.roles].sort((left, right) => {
      const leftMax = getMaxAdjustedScore(left.funds) ?? -Infinity;
      const rightMax = getMaxAdjustedScore(right.funds) ?? -Infinity;

      if (rightMax !== leftMax) {
        return rightMax - leftMax;
      }

      return left.role.localeCompare(right.role);
    });
  }, [data]);
  const resolvedActiveRole =
    sortedRoles.find((role) => role.role === selectedRole) ?? sortedRoles[0] ?? null;
  const allRankedFunds = useMemo(() => {
    const byId = new Map<string, RankedFund>();
    data?.roles.forEach((role) => role.funds.forEach((fund) => byId.set(fund.id, fund)));
    return [...byId.values()];
  }, [data]);
  const displayedHoldings = useMemo(() => {
    if (!activePortfolio) return [];
    const overrides = portfolioOverrides[activePortfolio.key] ?? {};
    return activePortfolio.holdings.map((holding) => {
      const replacementId = overrides[holding.requestedRole];
      const replacement = allRankedFunds.find((fund) => fund.id === replacementId);
      return replacement ? { ...holding, fund: replacement, matchedRole: replacement.fundClassUse ?? replacement.fundClass } : holding;
    });
  }, [activePortfolio, allRankedFunds, portfolioOverrides]);

  const roleChartData =
    activePortfolio?.allocationByRole.map((item, index) => ({
      ...item,
      color: getRoleColor(index)
    })) ?? [];

  const fundChartData =
    activePortfolio?.allocationByFund.map((item, index) => ({
      ...item,
      color: getRoleColor(index)
    })) ?? [];

  function updateControl<K extends keyof typeof DEFAULT_CONTROLS>(key: K, value: (typeof DEFAULT_CONTROLS)[K]) {
    startTransition(() => {
      setControls((current) => ({
        ...current,
        [key]: value
      }));
    });
  }

  function handleSortChange(key: SortKey) {
    if (sortKey === key) {
      setSortDirection((current) => (current === "desc" ? "asc" : "desc"));
      return;
    }

    setSortKey(key);
    setSortDirection(key === "name" || key === "ticker" ? "asc" : "desc");
  }

  function toggleCompare(fund: RankedFund) {
    setCompareFunds((current) =>
      current.some((item) => item.id === fund.id)
        ? current.filter((item) => item.id !== fund.id)
        : [...current, fund].slice(-5)
    );
  }

  function replaceHolding(requestedRole: string, fundId: string) {
    if (!activePortfolio) return;
    setPortfolioOverrides((current) => ({
      ...current,
      [activePortfolio.key]: {
        ...(current[activePortfolio.key] ?? {}),
        [requestedRole]: fundId
      }
    }));
  }

  const rankingControls: RankingControls = {
    baseRank: controls.baseRank,
    etfBonus: controls.etfBonus,
    noTransactionFeeBonus: controls.noTransactionFeeBonus,
    noLoadBonus: controls.noLoadBonus,
    lowExpenseExtraWeight: controls.lowExpenseExtraWeight
  };

  return (
    <main className="min-h-screen bg-[#F6F7F9] text-[#172033]">
      <header className="sticky top-0 z-30 border-b border-slate-200 bg-white/95 backdrop-blur">
        <div className="mx-auto flex max-w-[1720px] items-center justify-between px-4 py-4 md:px-6 lg:px-8">
          <button className="flex items-center gap-3 text-left" onClick={() => setAppView("recommendations")} type="button">
            <span className="grid h-9 w-9 place-items-center rounded-xl bg-[#176B5B] text-lg font-bold text-white">F</span>
            <span><b className="block text-[15px]">FundWise</b><span className="text-xs text-slate-500">Invest with context</span></span>
          </button>
          <nav className="flex items-center gap-1 rounded-xl bg-slate-100 p-1">
            {([
              ["recommendations", "Recommendations"],
              ["compare", `Compare${compareFunds.length ? ` (${compareFunds.length})` : ""}`],
              ["portfolio", "My Portfolio"]
            ] as [AppView, string][]).map(([key, label]) => (
              <button
                className={`rounded-lg px-3 py-2 text-sm font-semibold transition md:px-4 ${appView === key ? "bg-white text-slate-950 shadow-sm" : "text-slate-500 hover:text-slate-900"}`}
                key={key}
                onClick={() => { setAppView(key); setQuickViewFund(null); }}
                type="button"
              >
                {label}
              </button>
            ))}
          </nav>
        </div>
      </header>

      {appView === "compare" && (
        <section className="mx-auto max-w-[1440px] px-4 py-8 md:px-6 lg:px-8">
          <div className="section-title text-[#176B5B]">Compare Funds</div>
          <h1 className="mt-2 text-3xl font-semibold">Compare quality, cost and performance.</h1>
          <p className="mt-2 text-sm text-slate-500">Select funds from rankings or portfolio holdings. Up to five can be compared.</p>
          {compareFunds.length < 2 ? (
            <div className="mt-8 rounded-3xl border border-dashed border-slate-300 bg-white p-12 text-center">
              <p className="font-semibold">Select at least two funds to compare.</p>
              <button className="mt-4 rounded-xl bg-[#176B5B] px-4 py-3 text-sm font-semibold text-white" onClick={() => setAppView("recommendations")} type="button">Browse recommendations</button>
            </div>
          ) : (
            <div className="mt-8 overflow-x-auto rounded-3xl border border-slate-200 bg-white p-5 shadow-sm">
              <table className="w-full min-w-[780px] text-left text-sm">
                <thead><tr className="border-b border-slate-200"><th className="pb-4 text-slate-500">Metric</th>{compareFunds.map((fund) => <th className="pb-4" key={fund.id}><b>{fund.ticker}</b><p className="mt-1 max-w-[190px] text-xs font-normal text-slate-500">{fund.name}</p></th>)}</tr></thead>
                <tbody>
                  {[
                    ["Class", (fund: RankedFund) => fund.fundClassUse ?? fund.fundClass],
                    ["Adjusted score", (fund: RankedFund) => formatScore(fund.adjustedScore)],
                    ["Expense ratio", (fund: RankedFund) => formatPercent(fund.netExpenseRatio)],
                    ["3Y return", (fund: RankedFund) => formatPercent(fund.returns["3Y"])],
                    ["5Y return", (fund: RankedFund) => formatPercent(fund.returns["5Y"])],
                    ["10Y return", (fund: RankedFund) => formatPercent(fund.returns["10Y"])],
                    ["Risk-adjusted score", (fund: RankedFund) => formatScore(fund.riskAdjustedScore)]
                  ].map(([label, getter]) => (
                    <tr className="border-b border-slate-100 last:border-0" key={label as string}>
                      <td className="py-4 font-medium text-slate-500">{label as string}</td>
                      {compareFunds.map((fund) => <td className="py-4 font-semibold" key={fund.id}>{(getter as (fund: RankedFund) => string)(fund)}</td>)}
                    </tr>
                  ))}
                </tbody>
              </table>
              <div className="mt-5 rounded-xl bg-amber-50 p-4 text-sm text-amber-900">Holdings-overlap data will appear here when the holdings feed is connected.</div>
            </div>
          )}
        </section>
      )}

      {appView === "portfolio" && (
        <section className="mx-auto max-w-[1440px] px-4 py-8 md:px-6 lg:px-8">
          <div className="flex flex-col justify-between gap-4 md:flex-row md:items-end">
            <div><div className="section-title text-[#176B5B]">My Portfolio</div><h1 className="mt-2 text-3xl font-semibold">Your holdings and next best actions.</h1><p className="mt-2 text-sm text-slate-500">Add dollar amounts or percentages to receive allocation guidance.</p></div>
            <button className="rounded-xl bg-[#176B5B] px-4 py-3 text-sm font-semibold text-white" type="button">+ Add holding</button>
          </div>
          <div className="mt-8 grid gap-5 lg:grid-cols-[.9fr_1.3fr]">
            <div className="rounded-3xl border border-slate-200 bg-white p-6"><h2 className="font-semibold">Current allocation</h2><div className="mt-12 text-center text-sm text-slate-500">Add your funds and amounts to see allocation.</div></div>
            <div className="rounded-3xl border border-slate-200 bg-white p-6"><div className="section-title">Recommended Actions</div><h2 className="mt-2 text-xl font-semibold">Recommendations will appear here</h2><p className="mt-3 text-sm leading-6 text-slate-500">FundWise will review concentration, overlap, costs and target allocation before suggesting buy, sell, hold or rebalance actions.</p></div>
          </div>
        </section>
      )}

      {appView === "recommendations" && <div className="mx-auto max-w-[1720px] px-4 py-5 md:px-6 lg:px-8">
        <section className="flex flex-col gap-3 border-b border-slate-200 pb-4 sm:flex-row sm:items-center sm:justify-between">
          <div className="flex flex-wrap items-center gap-x-4 gap-y-2">
            <h1 className="text-sm font-semibold uppercase tracking-[0.1em] text-[#176B5B]">Fund Portfolio Builder</h1>
            <div className="hidden h-4 w-px bg-slate-300 sm:block" aria-hidden="true" />
            <div className="flex items-center gap-2" aria-label="Fund availability summary">
              <span className="inline-flex items-baseline gap-1.5 rounded-full border border-slate-200 bg-white px-3 py-1.5 text-xs text-slate-500 shadow-sm">
                Universe <strong className="text-sm text-slate-900">{data?.summary.totalFunds.toLocaleString() ?? "—"}</strong>
              </span>
              <span className="inline-flex items-baseline gap-1.5 rounded-full border border-emerald-200 bg-emerald-50 px-3 py-1.5 text-xs text-emerald-700">
                Eligible <strong className="text-sm text-emerald-950">{data?.summary.totalFilteredFunds.toLocaleString() ?? "—"}</strong>
              </span>
            </div>
          </div>
          <p className="flex shrink-0 items-center gap-2 text-xs text-slate-500" title={data?.summary.dataFile}>
            <span className="h-1.5 w-1.5 rounded-full bg-emerald-500" aria-hidden="true" />
            {formatFundsDataSource(data?.summary.dataFile)}
          </p>
        </section>

        <div className="mt-5 grid gap-6 xl:grid-cols-[320px_minmax(0,1fr)]">
          <aside className="glass-panel h-fit rounded-[30px] p-5">
            <div className="section-title">Controls</div>

            <div className="mt-5 space-y-5">
              <label className="block">
                <div className="mb-2 text-sm font-medium text-slate-800">Limit per role</div>
                <input
                  className="w-full rounded-2xl border border-slate-200 bg-white/80 px-3 py-2"
                  max={50}
                  min={5}
                  step={1}
                  type="number"
                  value={controls.limitPerRole}
                  onChange={(event) => updateControl("limitPerRole", Number(event.target.value))}
                />
              </label>

              <label className="block">
                <div className="mb-2 text-sm font-medium text-slate-800">Fund type filter</div>
                <select
                  className="w-full rounded-2xl border border-slate-200 bg-white/80 px-3 py-2"
                  value={controls.fundTypeMode}
                  onChange={(event) => updateControl("fundTypeMode", event.target.value as FundTypeMode)}
                >
                  <option value="both">Both</option>
                  <option value="ETF">ETF only</option>
                  <option value="Mutual Fund">Mutual fund only</option>
                </select>
              </label>

              <label className="block">
                <div className="mb-2 text-sm font-medium text-slate-800">Max expense ratio (%)</div>
                <input
                  className="w-full rounded-2xl border border-slate-200 bg-white/80 px-3 py-2"
                  placeholder="No limit"
                  step={0.05}
                  type="number"
                  value={controls.maxExpenseRatio}
                  onChange={(event) => updateControl("maxExpenseRatio", event.target.value)}
                />
              </label>

              <label className="block">
                <div className="mb-2 text-sm font-medium text-slate-800">ETF bonus</div>
                <input
                  className="w-full rounded-2xl border border-slate-200 bg-white/80 px-3 py-2"
                  step={0.5}
                  type="number"
                  value={controls.etfBonus}
                  onChange={(event) => updateControl("etfBonus", Number(event.target.value))}
                />
              </label>

              <label className="block">
                <div className="mb-2 text-sm font-medium text-slate-800">No-transaction-fee bonus</div>
                <input
                  className="w-full rounded-2xl border border-slate-200 bg-white/80 px-3 py-2"
                  step={0.5}
                  type="number"
                  value={controls.noTransactionFeeBonus}
                  onChange={(event) => updateControl("noTransactionFeeBonus", Number(event.target.value))}
                />
              </label>

              <label className="block">
                <div className="mb-2 text-sm font-medium text-slate-800">No-load bonus</div>
                <input
                  className="w-full rounded-2xl border border-slate-200 bg-white/80 px-3 py-2"
                  step={0.5}
                  type="number"
                  value={controls.noLoadBonus}
                  onChange={(event) => updateControl("noLoadBonus", Number(event.target.value))}
                />
              </label>

              <label className="block">
                <div className="mb-2 text-sm font-medium text-slate-800">Expense penalty weight</div>
                <input
                  className="w-full rounded-2xl border border-slate-200 bg-white/80 px-3 py-2"
                  step={0.25}
                  type="number"
                  value={controls.lowExpenseExtraWeight}
                  onChange={(event) => updateControl("lowExpenseExtraWeight", Number(event.target.value))}
                />
              </label>

              <label className="block">
                <div className="mb-2 text-sm font-medium text-slate-800">Base ranking</div>
                <select
                  className="w-full rounded-2xl border border-slate-200 bg-white/80 px-3 py-2"
                  value={controls.baseRank}
                  onChange={(event) => updateControl("baseRank", event.target.value as BaseRankField)}
                >
                  <option value="global_percentile">Global percentile</option>
                  <option value="global_score">Global score</option>
                </select>
              </label>
            </div>

            <div className="mt-6 rounded-[24px] bg-white/70 p-4 text-sm text-slate-600">
              <div className="font-semibold text-slate-900">Runtime ranking formula</div>
              <p className="mt-2">
                Adjusted score = selected base rank + ETF bonus + NTF bonus + no-load bonus - expense penalty.
              </p>
            </div>
          </aside>

          <section className="space-y-6">
            {loading && (
              <div className="glass-panel rounded-[30px] p-10 text-center">
                <div className="mx-auto h-10 w-10 animate-spin rounded-full border-4 border-slate-200 border-t-slate-900" />
                <p className="mt-4 text-sm text-slate-600">Loading and ranking the local fund universe…</p>
              </div>
            )}

            {!loading && error && (
              <div className="rounded-[30px] border border-rose-200 bg-rose-50 p-6 text-rose-900">
                <div className="text-lg font-semibold">Unable to load fund rankings</div>
                <p className="mt-2 text-sm">{error}</p>
              </div>
            )}

            {!loading && !error && data && data.roles.length === 0 && (
              <div className="glass-panel rounded-[30px] p-10 text-center">
                <div className="text-xl font-semibold text-slate-900">No funds match the current filters.</div>
                <p className="mt-2 text-sm text-slate-600">Try widening the expense ratio cap or switching fund type back to both.</p>
              </div>
            )}

            {!loading && !error && data && data.roles.length > 0 && (
              <>
                <div className="rounded-[30px] border border-slate-200 bg-white/84 p-5">
                  <div className="section-title">Roles</div>
                  <div className="mt-4 flex flex-wrap gap-2">
                    {sortedRoles.map((roleResult, index) => (
                      <button
                        key={roleResult.role}
                        className={`rounded-full border px-4 py-2 text-sm font-medium transition ${
                          roleResult.role === resolvedActiveRole?.role
                            ? "border-slate-900 bg-slate-900 text-white"
                            : "border-slate-200 bg-white text-slate-700"
                        }`}
                        onClick={() => setSelectedRole(roleResult.role)}
                        type="button"
                      >
                        <span
                          className="mr-2 inline-block h-2.5 w-2.5 rounded-full align-middle"
                          style={{ backgroundColor: getRoleColor(index) }}
                        />
                        {roleResult.role} ({roleResult.totalEligibleInRole}, {formatScore(getMaxAdjustedScore(roleResult.funds))})
                      </button>
                    ))}
                  </div>
                </div>

                {resolvedActiveRole && (
                  <RoleTable
                    direction={sortDirection}
                    onSortChange={handleSortChange}
                    onQuickView={setQuickViewFund}
                    onToggleCompare={toggleCompare}
                    rankingControls={rankingControls}
                    roleResult={resolvedActiveRole}
                    selectedFundIds={compareFunds.map((fund) => fund.id)}
                    sortKey={sortKey}
                  />
                )}

                <div className="rounded-[30px] border border-slate-200 bg-white/84 p-5">
                  <div className="section-title">Portfolios</div>
                  <div className="mt-4 grid gap-4 xl:grid-cols-3">
                    {data.portfolios.map((portfolio) => (
                      <PortfolioCard
                        key={portfolio.key}
                        portfolio={portfolio}
                        selected={portfolio.key === activePortfolio?.key}
                        onSelect={() => setSelectedPortfolioKey(portfolio.key)}
                      />
                    ))}
                  </div>
                </div>

                {activePortfolio && (
                  <div className="grid gap-6 2xl:grid-cols-[1.05fr_0.95fr]">
                    <div className="space-y-6">
                      <div className="glass-panel rounded-[30px] p-5">
                        <div className="section-title">Portfolio Holdings</div>
                        <div className="mt-4 space-y-3">
                          {displayedHoldings.map((holding) => (
                            <div key={`${activePortfolio.key}-${holding.requestedRole}-${holding.fund.id}`} className="rounded-[24px] bg-white/75 p-4">
                              <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
                                <div>
                                  <div className="text-xs uppercase tracking-[0.14em] text-slate-500">{holding.requestedRole}</div>
                                  <div className="mt-1 text-lg font-semibold text-slate-950">
                                    {holding.fund.ticker} <span className="font-normal text-slate-700">{holding.fund.name}</span>
                                  </div>
                                  <div className="mt-1 text-sm text-slate-600">Matched role: {holding.matchedRole}</div>
                                </div>
                                <div className="rounded-full bg-slate-900 px-3 py-1 text-sm font-semibold text-white">
                                  {holding.allocation}%
                                </div>
                              </div>
                              <div className="mt-3 flex flex-wrap gap-2 text-xs text-slate-600">
                                <span className="rounded-full bg-slate-100 px-2.5 py-1">Expense {formatPercent(holding.fund.netExpenseRatio)}</span>
                                <span className="rounded-full bg-slate-100 px-2.5 py-1">Adjusted {formatScore(holding.fund.adjustedScore)}</span>
                                <span className="rounded-full bg-slate-100 px-2.5 py-1">Role % {formatScore(holding.fund.baseRolePercentile)}</span>
                              </div>
                              <div className="mt-3 text-sm text-slate-600">
                                {holding.fund.reasons.length > 0 ? holding.fund.reasons.join(" • ") : "No additional ranking reasons"}
                              </div>
                              <div className="mt-4 flex flex-col gap-2 sm:flex-row">
                                <label className="flex-1">
                                  <span className="sr-only">Replace {holding.fund.ticker}</span>
                                  <select
                                    className="w-full rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm font-semibold"
                                    onChange={(event) => replaceHolding(holding.requestedRole, event.target.value)}
                                    value={holding.fund.id}
                                  >
                                    {(
                                      sortedRoles.find((role) => role.role === holding.matchedRole)?.funds ??
                                      sortedRoles.find((role) => role.role === holding.requestedRole)?.funds ??
                                      allRankedFunds
                                    ).map((fund) => <option key={fund.id} value={fund.id}>{fund.ticker} — {fund.name}</option>)}
                                  </select>
                                </label>
                                <button className="rounded-xl border border-slate-200 px-3 py-2 text-sm font-semibold" onClick={() => toggleCompare(holding.fund)} type="button">
                                  {compareFunds.some((fund) => fund.id === holding.fund.id) ? "Compared ✓" : "Compare"}
                                </button>
                                <button className="rounded-xl border border-slate-200 px-3 py-2 text-sm font-semibold" onClick={() => setQuickViewFund(holding.fund)} type="button">Quick view</button>
                              </div>
                              {holding.warning && (
                                <div className="mt-3 rounded-2xl border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-900">
                                  {holding.warning}
                                </div>
                              )}
                            </div>
                          ))}
                        </div>
                      </div>

                      <div className="glass-panel rounded-[30px] p-5">
                        <div className="section-title">Portfolio Metrics</div>
                        <div className="mt-4 grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
                          <div className="metric-card">
                            <div className="metric-label">Weighted expense ratio</div>
                            <div className="metric-value">{formatPercent(activePortfolio.metrics.weightedExpenseRatio)}</div>
                          </div>
                          <div className="metric-card">
                            <div className="metric-label">Weighted return score</div>
                            <div className="metric-value">{formatScore(activePortfolio.metrics.weightedReturnScore)}</div>
                          </div>
                          <div className="metric-card">
                            <div className="metric-label">Cost score</div>
                            <div className="metric-value">{formatScore(activePortfolio.metrics.costScore)}</div>
                          </div>
                          <div className="metric-card">
                            <div className="metric-label">Risk-adjusted score</div>
                            <div className="metric-value">{formatScore(activePortfolio.metrics.riskAdjustedScore)}</div>
                          </div>
                          <div className="metric-card">
                            <div className="metric-label">Volatility score</div>
                            <div className="metric-value">{formatScore(activePortfolio.metrics.volatilityScore)}</div>
                          </div>
                          <div className="metric-card">
                            <div className="metric-label">Overall portfolio score</div>
                            <div className="metric-value">{formatScore(activePortfolio.metrics.portfolioScore)}</div>
                          </div>
                        </div>
                        {activePortfolio.warnings.length > 0 && (
                          <div className="mt-4 rounded-[24px] border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-900">
                            {activePortfolio.warnings.map((warning) => (
                              <div key={warning}>{warning}</div>
                            ))}
                          </div>
                        )}
                      </div>
                    </div>

                    <div className="space-y-6">
                      <div className="glass-panel rounded-[30px] p-5">
                        <div className="section-title">Allocation By Fund</div>
                        <div className="mt-4 h-72 rounded-[24px] bg-white/72 p-2">
                          <ResponsiveContainer width="100%" height="100%">
                            <PieChart>
                              <Pie data={fundChartData} dataKey="allocation" nameKey="ticker" innerRadius={62} outerRadius={104} paddingAngle={2}>
                                {fundChartData.map((entry) => (
                                  <Cell key={entry.ticker} fill={entry.color} />
                                ))}
                              </Pie>
                              <Tooltip formatter={(value: number) => `${value}%`} />
                            </PieChart>
                          </ResponsiveContainer>
                        </div>
                      </div>

                      <div className="glass-panel rounded-[30px] p-5">
                        <div className="section-title">Allocation By Role</div>
                        <div className="mt-4 h-72 rounded-[24px] bg-white/72 p-2">
                          <ResponsiveContainer width="100%" height="100%">
                            <BarChart data={roleChartData}>
                              <XAxis dataKey="role" angle={-18} textAnchor="end" height={80} fontSize={11} />
                              <YAxis unit="%" fontSize={11} />
                              <Tooltip formatter={(value: number) => `${value}%`} />
                              <Bar dataKey="allocation" radius={[10, 10, 0, 0]}>
                                {roleChartData.map((entry) => (
                                  <Cell key={entry.role} fill={entry.color} />
                                ))}
                              </Bar>
                            </BarChart>
                          </ResponsiveContainer>
                        </div>
                      </div>

                      <div className="glass-panel rounded-[30px] p-5">
                        <div className="section-title">Component Score Bars</div>
                        <div className="mt-4 h-72 rounded-[24px] bg-white/72 p-2">
                          <ResponsiveContainer width="100%" height="100%">
                            <BarChart data={activePortfolio.componentScoreBars}>
                              <XAxis dataKey="label" fontSize={11} />
                              <YAxis domain={[0, 100]} fontSize={11} />
                              <Tooltip formatter={(value: number) => formatScore(value)} />
                              <Bar dataKey="value" radius={[10, 10, 0, 0]} fill="#0f766e" />
                            </BarChart>
                          </ResponsiveContainer>
                        </div>
                      </div>
                    </div>
                  </div>
                )}
              </>
            )}
          </section>
        </div>
      </div>}

      {appView === "recommendations" && compareFunds.length >= 2 && (
        <div className="fixed bottom-5 left-1/2 z-20 flex w-[calc(100%-2rem)] max-w-xl -translate-x-1/2 items-center justify-between rounded-2xl bg-[#172033] px-4 py-3 text-white shadow-2xl">
          <div><b className="text-sm">{compareFunds.length} funds selected</b><p className="text-xs text-slate-300">{compareFunds.map((fund) => fund.ticker).join(" · ")}</p></div>
          <button className="rounded-xl bg-white px-4 py-2 text-sm font-bold text-slate-900" onClick={() => setAppView("compare")} type="button">Compare funds →</button>
        </div>
      )}

      {quickViewFund && (
        <div className="fixed inset-0 z-40 bg-slate-950/30" onClick={() => setQuickViewFund(null)}>
          <aside className="ml-auto h-full w-full max-w-[580px] overflow-y-auto bg-white p-6 shadow-2xl" onClick={(event) => event.stopPropagation()}>
            <div className="flex items-start justify-between">
              <div><span className="rounded-full bg-emerald-50 px-2 py-1 text-xs font-bold text-[#176B5B]">{quickViewFund.fundClassUse ?? quickViewFund.fundClass}</span><h2 className="mt-3 text-2xl font-semibold">{quickViewFund.ticker}</h2><p className="text-sm text-slate-500">{quickViewFund.name}</p></div>
              <button aria-label="Close" className="grid h-10 w-10 place-items-center rounded-full bg-slate-100 text-xl" onClick={() => setQuickViewFund(null)} type="button">×</button>
            </div>
            <div className="mt-6 grid grid-cols-3 gap-3">
              {[["Score", formatScore(quickViewFund.adjustedScore)], ["Expense", formatPercent(quickViewFund.netExpenseRatio)], ["5Y return", formatPercent(quickViewFund.returns["5Y"])]].map(([label, value]) => <div className="rounded-xl bg-slate-50 p-3" key={label}><p className="text-xs text-slate-500">{label}</p><b className="mt-1 block">{value}</b></div>)}
            </div>
            <div className="mt-7"><h3 className="font-semibold">Historical NAV</h3><p className="text-xs text-slate-500">Illustrative history · connect NAV feed for live values</p></div>
            <div className="mt-4 h-64">
              <ResponsiveContainer><AreaChart data={[{y:"2019",v:100},{y:"2020",v:112},{y:"2021",v:139},{y:"2022",v:121},{y:"2023",v:146},{y:"2024",v:169},{y:"Now",v:184}]}><XAxis dataKey="y" axisLine={false} tickLine={false} fontSize={11}/><YAxis hide/><Tooltip/><Area dataKey="v" fill="#D7EFE9" stroke="#176B5B" strokeWidth={3} type="monotone"/></AreaChart></ResponsiveContainer>
            </div>
            <div className="rounded-xl border border-emerald-200 bg-emerald-50 p-4"><b className="text-emerald-900">Historical context: Neutral</b><p className="mt-2 text-sm leading-6 text-emerald-900/75">Use valuation, drawdown and your allocation—not NAV alone—to decide whether to buy.</p></div>
            <div className="mt-5 grid grid-cols-2 gap-3"><button className="rounded-xl border border-slate-200 py-3 font-semibold" onClick={() => toggleCompare(quickViewFund)} type="button">{compareFunds.some((fund) => fund.id === quickViewFund.id) ? "Remove from compare" : "Add to compare"}</button><button className="rounded-xl bg-[#176B5B] py-3 font-semibold text-white" onClick={() => setAppView("portfolio")} type="button">Add to portfolio</button></div>
          </aside>
        </div>
      )}
    </main>
  );
}
