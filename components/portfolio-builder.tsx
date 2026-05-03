"use client";

import { useDeferredValue, useMemo, useState } from "react";
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
import { buildPortfolioRecommendations } from "@/lib/portfolio-engine";
import type { BuilderConstraints, FundRecord, PortfolioRecommendation, ScoredFund } from "@/lib/types";

const ROLE_COLORS: Record<string, string> = {
  "US Large Cap": "#1d4ed8",
  "US Extended Market / Mid-Small Cap": "#2563eb",
  "International Developed Equity": "#0f766e",
  "Emerging Markets Equity": "#14b8a6",
  "Sector / Thematic Equity": "#f59e0b",
  "Core Bond": "#6366f1",
  "Short-Term Bond": "#8b5cf6",
  "TIPS / Inflation-Protected Bond": "#a855f7",
  "Other / Excluded": "#94a3b8"
};

const DEFAULT_CONSTRAINTS: BuilderConstraints = {
  stockAllocation: 60,
  fundTarget: 6,
  maxExpenseRatio: 1,
  excludeSectorThematic: true,
  lowCostPriority: true
};

function formatPercent(value?: number, digits = 2) {
  return value === undefined ? "N/A" : `${value.toFixed(digits)}%`;
}

function formatRole(role: string) {
  return role.replace(" / ", " /\n");
}

function MetricRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="metric-chip rounded-2xl px-3 py-2">
      <div className="text-[11px] uppercase tracking-[0.16em] text-slate-500">{label}</div>
      <div className="mt-1 text-sm font-semibold text-slate-900">{value}</div>
    </div>
  );
}

function PortfolioCard({ portfolio, active, onSelect }: { portfolio: PortfolioRecommendation; active: boolean; onSelect: () => void }) {
  return (
    <button
      className={`glass-panel rounded-[28px] p-5 text-left transition duration-200 ${
        active ? "border-blue-400 shadow-[0_24px_64px_rgba(29,78,216,0.2)]" : "hover:-translate-y-0.5"
      }`}
      onClick={onSelect}
      type="button"
    >
      <div className="flex items-start justify-between gap-4">
        <div>
          <div className="section-title">{portfolio.name}</div>
          <h2 className="mt-2 text-xl font-semibold">{portfolio.description}</h2>
        </div>
        <div className="rounded-full bg-blue-600 px-3 py-1 text-xs font-semibold text-white">
          {portfolio.selectedFunds.length} funds
        </div>
      </div>

      <div className="mt-5 grid grid-cols-2 gap-3">
        <MetricRow label="Stock / Bond" value={`${portfolio.stockAllocation}% / ${portfolio.bondAllocation}%`} />
        <MetricRow label="Weighted Fee" value={formatPercent(portfolio.weightedMetrics.expenseRatio)} />
        <MetricRow label="Weighted 5Y" value={formatPercent(portfolio.weightedMetrics.return5Y)} />
        <MetricRow label="Risk" value={formatPercent(portfolio.weightedMetrics.standardDeviation)} />
      </div>

      <div className="mt-5 space-y-2">
        {portfolio.selectedFunds.map((selection) => (
          <div key={`${portfolio.key}-${selection.role}-${selection.fund.id}`} className="flex items-center justify-between gap-3 rounded-2xl bg-white/70 px-3 py-2">
            <div className="min-w-0">
              <div className="truncate text-sm font-semibold">
                {selection.fund.ticker} <span className="font-normal text-slate-600">{selection.fund.name}</span>
              </div>
              <div className="text-xs text-slate-500">{selection.role}</div>
            </div>
            <div className="text-sm font-semibold text-slate-900">{selection.allocation}%</div>
          </div>
        ))}
      </div>
    </button>
  );
}

function RoleCandidateTable({ rankedCandidates }: { rankedCandidates: Record<string, ScoredFund[]> }) {
  const entries = Object.entries(rankedCandidates).filter(
    ([role, funds]) => role !== "Other / Excluded" && funds.length > 0
  );

  return (
    <div className="space-y-5">
      {entries.map(([role, funds]) => (
        <div key={role} className="glass-panel rounded-[24px] p-4">
          <div className="flex items-center justify-between gap-3">
            <div>
              <div className="section-title">Top Candidates</div>
              <h3 className="mt-1 text-lg font-semibold">{role}</h3>
            </div>
            <div className="rounded-full px-3 py-1 text-xs font-semibold text-slate-600" style={{ backgroundColor: `${ROLE_COLORS[role]}20` }}>
              {funds.length} eligible
            </div>
          </div>

          <div className="mt-4 overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead className="text-left text-xs uppercase tracking-[0.14em] text-slate-500">
                <tr>
                  <th className="pb-2 pr-4">Fund</th>
                  <th className="pb-2 pr-4">Score</th>
                  <th className="pb-2 pr-4">Expense</th>
                  <th className="pb-2 pr-4">5Y</th>
                  <th className="pb-2 pr-4">Risk</th>
                  <th className="pb-2">Top Reasons</th>
                </tr>
              </thead>
              <tbody>
                {funds.slice(0, 5).map((fund) => (
                  <tr key={fund.id} className="border-t border-slate-200/70 align-top">
                    <td className="py-3 pr-4">
                      <div className="font-semibold">{fund.ticker}</div>
                      <div className="max-w-sm text-slate-600">{fund.name}</div>
                    </td>
                    <td className="py-3 pr-4 font-semibold">{fund.score.toFixed(1)}</td>
                    <td className="py-3 pr-4">{formatPercent(fund.expenseRatio)}</td>
                    <td className="py-3 pr-4">{formatPercent(fund.returns["5Y"])}</td>
                    <td className="py-3 pr-4">{formatPercent(fund.standardDeviation)}</td>
                    <td className="py-3">
                      <div className="space-y-1 text-slate-600">
                        {fund.selectionReasons.slice(0, 2).map((reason) => (
                          <div key={reason}>• {reason}</div>
                        ))}
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      ))}
    </div>
  );
}

export function PortfolioBuilder({
  funds,
  warnings,
  rowCount,
  csvPath
}: {
  funds: FundRecord[];
  warnings: string[];
  rowCount: number;
  csvPath: string;
}) {
  const [constraints, setConstraints] = useState<BuilderConstraints>(DEFAULT_CONSTRAINTS);
  const [selectedPortfolioKey, setSelectedPortfolioKey] = useState<PortfolioRecommendation["key"]>("balanced");
  const deferredConstraints = useDeferredValue(constraints);

  const { recommendations, rankedCandidates } = useMemo(
    () => buildPortfolioRecommendations(funds, deferredConstraints),
    [funds, deferredConstraints]
  );

  const selectedPortfolio =
    recommendations.find((portfolio) => portfolio.key === selectedPortfolioKey) ?? recommendations[1];

  const allocationChartData = selectedPortfolio?.selectedFunds.map((selection) => ({
    name: selection.fund.ticker,
    value: selection.allocation,
    role: selection.role
  })) ?? [];

  const roleChartData = selectedPortfolio?.roleAllocation ?? [];

  return (
    <main className="min-h-screen px-4 py-6 md:px-6 lg:px-8">
      <div className="mx-auto max-w-[1680px]">
        <div className="glass-panel rounded-[32px] p-6 md:p-8">
          <div className="flex flex-col gap-3 lg:flex-row lg:items-end lg:justify-between">
            <div>
              <div className="section-title">Local Fidelity Portfolio Builder</div>
              <h1 className="mt-2 max-w-4xl text-3xl font-semibold leading-tight md:text-5xl">
                Deterministic portfolio construction from your local Fidelity fund universe.
              </h1>
              <p className="mt-3 max-w-3xl text-sm text-slate-600 md:text-base">
                This app ranks eligible funds by portfolio role, assembles simple model portfolios, and explains the tradeoffs without using external APIs or optimization black boxes.
              </p>
            </div>
            <div className="rounded-[24px] bg-white/75 px-4 py-3 text-sm text-slate-600">
              <div><span className="font-semibold text-slate-900">{rowCount.toLocaleString()}</span> parsed funds</div>
              <div className="truncate">CSV: {csvPath}</div>
            </div>
          </div>

          {warnings.length > 0 && (
            <div className="mt-5 rounded-[24px] border border-amber-300 bg-amber-50 px-4 py-3 text-sm text-amber-900">
              <div className="font-semibold">Data warnings</div>
              <div className="mt-2 space-y-1">
                {warnings.map((warning) => (
                  <div key={warning}>• {warning}</div>
                ))}
              </div>
            </div>
          )}
        </div>

        <div className="mt-6 grid gap-6 xl:grid-cols-[320px_minmax(0,1fr)_360px]">
          <aside className="glass-panel rounded-[28px] p-5 h-fit">
            <div className="section-title">Constraints</div>
            <div className="mt-4 space-y-5">
              <label className="block">
                <div className="flex items-center justify-between text-sm font-medium">
                  <span>Stock allocation</span>
                  <span>{constraints.stockAllocation}%</span>
                </div>
                <input
                  className="mt-2 w-full"
                  type="range"
                  min={20}
                  max={90}
                  step={5}
                  value={constraints.stockAllocation}
                  onChange={(event) =>
                    setConstraints((current) => ({
                      ...current,
                      stockAllocation: Number(event.target.value)
                    }))
                  }
                />
                <div className="mt-1 text-xs text-slate-500">Bond allocation auto-derives to {100 - constraints.stockAllocation}%.</div>
              </label>

              <label className="block">
                <div className="flex items-center justify-between text-sm font-medium">
                  <span>Target fund count</span>
                  <span>{constraints.fundTarget}</span>
                </div>
                <input
                  className="mt-2 w-full"
                  type="range"
                  min={5}
                  max={8}
                  step={1}
                  value={constraints.fundTarget}
                  onChange={(event) =>
                    setConstraints((current) => ({
                      ...current,
                      fundTarget: Number(event.target.value)
                    }))
                  }
                />
                <div className="mt-1 text-xs text-slate-500">Simple and granular portfolios adjust around this target.</div>
              </label>

              <label className="block">
                <div className="text-sm font-medium">Max expense ratio (%)</div>
                <input
                  className="mt-2 w-full rounded-2xl border border-slate-200 bg-white/80 px-3 py-2"
                  min={0.05}
                  max={2.5}
                  step={0.05}
                  type="number"
                  value={constraints.maxExpenseRatio}
                  onChange={(event) =>
                    setConstraints((current) => ({
                      ...current,
                      maxExpenseRatio: Number(event.target.value)
                    }))
                  }
                />
              </label>

              <label className="flex cursor-pointer items-center justify-between gap-3 rounded-2xl bg-white/70 px-3 py-3">
                <div>
                  <div className="text-sm font-medium">Exclude sector / thematic</div>
                  <div className="text-xs text-slate-500">Avoid concentrated equity sleeves by default.</div>
                </div>
                <input
                  checked={constraints.excludeSectorThematic}
                  onChange={(event) =>
                    setConstraints((current) => ({
                      ...current,
                      excludeSectorThematic: event.target.checked
                    }))
                  }
                  type="checkbox"
                />
              </label>

              <label className="flex cursor-pointer items-center justify-between gap-3 rounded-2xl bg-white/70 px-3 py-3">
                <div>
                  <div className="text-sm font-medium">Low-cost priority</div>
                  <div className="text-xs text-slate-500">Increase the scoring weight on expense ratio.</div>
                </div>
                <input
                  checked={constraints.lowCostPriority}
                  onChange={(event) =>
                    setConstraints((current) => ({
                      ...current,
                      lowCostPriority: event.target.checked
                    }))
                  }
                  type="checkbox"
                />
              </label>
            </div>
          </aside>

          <section className="space-y-6">
            <div className="grid gap-5 xl:grid-cols-3">
              {recommendations.map((portfolio) => (
                <PortfolioCard
                  key={portfolio.key}
                  portfolio={portfolio}
                  active={portfolio.key === selectedPortfolio?.key}
                  onSelect={() => setSelectedPortfolioKey(portfolio.key)}
                />
              ))}
            </div>

            {selectedPortfolio && (
              <div className="grid gap-6 lg:grid-cols-[1.1fr_0.9fr]">
                <div className="glass-panel rounded-[28px] p-5">
                  <div className="section-title">Allocation View</div>
                  <div className="mt-3 grid gap-5 lg:grid-cols-2">
                    <div className="h-72 rounded-[24px] bg-white/60 p-2">
                      <ResponsiveContainer width="100%" height="100%">
                        <PieChart>
                          <Pie data={allocationChartData} dataKey="value" nameKey="name" innerRadius={62} outerRadius={98} paddingAngle={2}>
                            {allocationChartData.map((entry) => (
                              <Cell key={entry.name} fill={ROLE_COLORS[entry.role]} />
                            ))}
                          </Pie>
                          <Tooltip formatter={(value: number) => `${value}%`} />
                        </PieChart>
                      </ResponsiveContainer>
                    </div>
                    <div className="h-72 rounded-[24px] bg-white/60 p-2">
                      <ResponsiveContainer width="100%" height="100%">
                        <BarChart data={roleChartData}>
                          <XAxis dataKey="role" tickFormatter={formatRole} interval={0} fontSize={11} />
                          <YAxis unit="%" fontSize={11} />
                          <Tooltip formatter={(value: number) => `${value}%`} />
                          <Bar dataKey="allocation" radius={[12, 12, 0, 0]}>
                            {roleChartData.map((entry) => (
                              <Cell key={entry.role} fill={ROLE_COLORS[entry.role]} />
                            ))}
                          </Bar>
                        </BarChart>
                      </ResponsiveContainer>
                    </div>
                  </div>
                </div>

                <div className="glass-panel rounded-[28px] p-5">
                  <div className="section-title">Weighted Metrics</div>
                  <div className="mt-4 grid grid-cols-2 gap-3">
                    <MetricRow label="Weighted Expense" value={formatPercent(selectedPortfolio.weightedMetrics.expenseRatio)} />
                    <MetricRow label="Weighted 1Y" value={formatPercent(selectedPortfolio.weightedMetrics.return1Y)} />
                    <MetricRow label="Weighted 3Y" value={formatPercent(selectedPortfolio.weightedMetrics.return3Y)} />
                    <MetricRow label="Weighted 5Y" value={formatPercent(selectedPortfolio.weightedMetrics.return5Y)} />
                    <MetricRow label="Weighted 10Y" value={formatPercent(selectedPortfolio.weightedMetrics.return10Y)} />
                    <MetricRow label="Weighted Std Dev" value={formatPercent(selectedPortfolio.weightedMetrics.standardDeviation)} />
                  </div>

                  <div className="mt-5 rounded-[24px] bg-white/70 p-4">
                    <div className="font-semibold">Risk summary</div>
                    <p className="mt-2 text-sm text-slate-600">{selectedPortfolio.riskSummary}</p>
                  </div>
                </div>
              </div>
            )}

            <RoleCandidateTable rankedCandidates={rankedCandidates} />
          </section>

          <aside className="space-y-6">
            {selectedPortfolio && (
              <>
                <div className="glass-panel rounded-[28px] p-5">
                  <div className="section-title">Why This Portfolio?</div>
                  <div className="mt-4 space-y-4">
                    {selectedPortfolio.selectedFunds.map((selection) => (
                      <div key={`${selection.role}-${selection.fund.id}`} className="rounded-[24px] bg-white/72 p-4">
                        <div className="flex items-start justify-between gap-3">
                          <div>
                            <div className="text-sm text-slate-500">{selection.role}</div>
                            <div className="text-base font-semibold">
                              {selection.fund.name} ({selection.fund.ticker})
                            </div>
                          </div>
                          <div className="rounded-full bg-slate-900 px-3 py-1 text-xs font-semibold text-white">
                            {selection.allocation}%
                          </div>
                        </div>
                        <div className="mt-3 space-y-1 text-sm text-slate-600">
                          {selection.explanation.map((item) => (
                            <div key={item}>• {item}</div>
                          ))}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>

                <div className="glass-panel rounded-[28px] p-5">
                  <div className="section-title">Tradeoffs And Risks</div>
                  <div className="mt-4 space-y-2 text-sm text-slate-600">
                    {selectedPortfolio.tradeoffs.map((tradeoff) => (
                      <div key={tradeoff}>• {tradeoff}</div>
                    ))}
                    <div>• This tool is for portfolio construction research and comparison, not personalized investment advice.</div>
                  </div>
                </div>
              </>
            )}
          </aside>
        </div>
      </div>
    </main>
  );
}
