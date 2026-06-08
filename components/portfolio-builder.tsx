"use client";

import { startTransition, useDeferredValue, useEffect, useMemo, useState } from "react";
import {
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
type SortKey =
  | "rank"
  | "ticker"
  | "name"
  | "rolePercentile"
  | "globalPercentile"
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
  rankingControls
}: {
  roleResult: TopFundsByRoleResult;
  sortKey: SortKey;
  direction: "asc" | "desc";
  onSortChange: (key: SortKey) => void;
  rankingControls: RankingControls;
}) {
  const sortedFunds = useMemo(
    () => sortFunds(roleResult.funds, sortKey, direction),
    [direction, roleResult.funds, sortKey]
  );

  return (
    <div className="rounded-[28px] border border-slate-200/80 bg-white/80 p-4 shadow-[0_20px_40px_rgba(15,23,42,0.06)]">
      <div className="flex flex-col gap-3 md:flex-row md:items-end md:justify-between">
        <div>
          <div className="section-title">Top Funds By Role</div>
          <h2 className="mt-1 text-2xl font-semibold text-slate-950">{roleResult.role}</h2>
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
        <table className="min-w-[1400px] text-sm">
          <thead className="sticky top-0 bg-white/95 backdrop-blur-sm">
            <tr className="border-b border-slate-200 text-left">
              <th className="pb-3 pr-4"><SortButton label="Rank" sortKey="rank" activeKey={sortKey} direction={direction} onChange={onSortChange} /></th>
              <th className="pb-3 pr-4"><SortButton label="Ticker" sortKey="ticker" activeKey={sortKey} direction={direction} onChange={onSortChange} /></th>
              <th className="pb-3 pr-4"><SortButton label="Name" sortKey="name" activeKey={sortKey} direction={direction} onChange={onSortChange} /></th>
              <th className="pb-3 pr-4"><SortButton label="Role %" sortKey="rolePercentile" activeKey={sortKey} direction={direction} onChange={onSortChange} /></th>
              <th className="pb-3 pr-4"><SortButton label="Global %" sortKey="globalPercentile" activeKey={sortKey} direction={direction} onChange={onSortChange} /></th>
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
              <th className="pb-3">Why It Ranks</th>
            </tr>
          </thead>
          <tbody>
            {sortedFunds.map((fund) => (
              <tr key={fund.id} className="border-b border-slate-100 align-top last:border-b-0">
                <td className="py-3 pr-4 font-semibold text-slate-900">{fund.rank}</td>
                <td className="py-3 pr-4 font-semibold text-slate-900">{fund.ticker}</td>
                <td className="py-3 pr-4 min-w-[280px] text-slate-700">{fund.name}</td>
                <td className="py-3 pr-4">{formatScore(fund.baseRolePercentile)}</td>
                <td className="py-3 pr-4">{formatScore(fund.globalPercentile)}</td>
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
                <td className="py-3 min-w-[260px] text-slate-600">
                  {fund.reasons.length > 0 ? fund.reasons.join(" • ") : "No notable flags"}
                </td>
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
  const [controls, setControls] = useState(DEFAULT_CONTROLS);
  const deferredControls = useDeferredValue(controls);
  const [data, setData] = useState<TopByRoleResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedRole, setSelectedRole] = useState<string | null>(null);
  const [selectedPortfolioKey, setSelectedPortfolioKey] = useState<Portfolio["key"]>("5-fund");
  const [sortKey, setSortKey] = useState<SortKey>("adjustedScore");
  const [sortDirection, setSortDirection] = useState<"asc" | "desc">("desc");

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

  const rankingControls: RankingControls = {
    baseRank: controls.baseRank,
    etfBonus: controls.etfBonus,
    noTransactionFeeBonus: controls.noTransactionFeeBonus,
    noLoadBonus: controls.noLoadBonus,
    lowExpenseExtraWeight: controls.lowExpenseExtraWeight
  };

  return (
    <main className="min-h-screen px-4 py-6 md:px-6 lg:px-8">
      <div className="mx-auto max-w-[1720px]">
        <section className="hero-panel rounded-[36px] p-4 md:p-5">
          <div className="flex flex-col gap-3 xl:flex-row xl:items-center xl:justify-between">
            <div className="section-title text-emerald-800">Fund Portfolio Builder</div>
            <div className="grid gap-3 sm:grid-cols-3">
              <div className="summary-card">
                <div className="summary-label">Universe</div>
                <div className="summary-value">{data?.summary.totalFunds.toLocaleString() ?? "—"}</div>
              </div>
              <div className="summary-card">
                <div className="summary-label">Eligible</div>
                <div className="summary-value">{data?.summary.totalFilteredFunds.toLocaleString() ?? "—"}</div>
              </div>
              <div className="summary-card">
                <div className="summary-label">CSV</div>
                <div className="truncate text-sm font-medium text-slate-700" title={data?.summary.dataFile ?? "Loading"}>
                  {data ? data.summary.dataFile.split("/").slice(-2).join("/") : "Loading"}
                </div>
              </div>
            </div>
          </div>
        </section>

        <div className="mt-6 grid gap-6 xl:grid-cols-[320px_minmax(0,1fr)]">
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
                    rankingControls={rankingControls}
                    roleResult={resolvedActiveRole}
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
                          {activePortfolio.holdings.map((holding) => (
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
      </div>
    </main>
  );
}
