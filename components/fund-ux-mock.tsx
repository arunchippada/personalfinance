"use client";

import { useMemo, useState } from "react";
import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis
} from "recharts";

type View = "recommendations" | "compare" | "portfolio";

const funds = [
  { ticker: "FXAIX", name: "Fidelity 500 Index Fund", category: "Large Blend", score: 96, expense: "0.015%", returns: "14.84%", risk: "Average", color: "#176B5B" },
  { ticker: "FSPGX", name: "Fidelity Large Cap Growth Index", category: "Large Growth", score: 94, expense: "0.035%", returns: "17.62%", risk: "Above avg.", color: "#4C67E8" },
  { ticker: "FSKAX", name: "Fidelity Total Market Index", category: "Large Blend", score: 92, expense: "0.015%", returns: "13.96%", risk: "Average", color: "#D58A36" },
  { ticker: "FZROX", name: "Fidelity ZERO Total Market", category: "Large Blend", score: 91, expense: "0.000%", returns: "13.81%", risk: "Average", color: "#8B5CF6" },
  { ticker: "FTIHX", name: "Fidelity Total International Index", category: "Foreign Large Blend", score: 88, expense: "0.060%", returns: "7.48%", risk: "Average", color: "#D05A72" }
];

const navData = [
  { year: "2019", nav: 98 }, { year: "2020", nav: 112 }, { year: "2021", nav: 142 },
  { year: "2022", nav: 118 }, { year: "2023", nav: 145 }, { year: "2024", nav: 168 },
  { year: "Now", nav: 184 }
];

const overlapData = [
  { name: "Shared", value: 72, color: "#176B5B" },
  { name: "Only FXAIX", value: 18, color: "#4C67E8" },
  { name: "Only FSKAX", value: 10, color: "#D8DDE6" }
];

const portfolioData = [
  { name: "FXAIX", value: 48, amount: "$58,400", color: "#176B5B" },
  { name: "FSPGX", value: 27, amount: "$32,850", color: "#4C67E8" },
  { name: "FTIHX", value: 15, amount: "$18,250", color: "#D58A36" },
  { name: "FXNAX", value: 10, amount: "$12,170", color: "#D05A72" }
];

export function FundUxMock() {
  const [view, setView] = useState<View>("recommendations");
  const [detailOpen, setDetailOpen] = useState(false);
  const [selected, setSelected] = useState<string[]>(["FXAIX", "FSKAX"]);

  const selectedFunds = useMemo(
    () => funds.filter((fund) => selected.includes(fund.ticker)),
    [selected]
  );

  function toggleFund(ticker: string) {
    setSelected((current) =>
      current.includes(ticker) ? current.filter((item) => item !== ticker) : [...current, ticker]
    );
  }

  return (
    <main className="min-h-screen bg-[#F6F7F9] text-[#172033]">
      <header className="sticky top-0 z-30 border-b border-slate-200 bg-white/95 backdrop-blur">
        <div className="mx-auto flex max-w-[1440px] items-center justify-between px-5 py-4 lg:px-8">
          <button className="flex items-center gap-3 text-left" onClick={() => setView("recommendations")}>
            <span className="grid h-9 w-9 place-items-center rounded-xl bg-[#176B5B] text-lg font-bold text-white">F</span>
            <span><b className="block text-[15px]">FundWise</b><span className="text-xs text-slate-500">Invest with context</span></span>
          </button>
          <nav className="hidden items-center gap-1 rounded-xl bg-slate-100 p-1 md:flex">
            {([
              ["recommendations", "Recommendations"],
              ["compare", `Compare${selected.length ? ` (${selected.length})` : ""}`],
              ["portfolio", "My Portfolio"]
            ] as [View, string][]).map(([key, label]) => (
              <button
                key={key}
                onClick={() => { setView(key); setDetailOpen(false); }}
                className={`rounded-lg px-4 py-2 text-sm font-semibold transition ${view === key ? "bg-white text-slate-950 shadow-sm" : "text-slate-500 hover:text-slate-900"}`}
              >
                {label}
              </button>
            ))}
          </nav>
          <button className="rounded-xl border border-slate-200 bg-white px-4 py-2 text-sm font-semibold shadow-sm">Saved lists</button>
        </div>
      </header>

      <div className="mx-auto max-w-[1440px] px-5 py-8 lg:px-8">
        {view === "recommendations" && (
          <>
            <div className="flex flex-col justify-between gap-5 lg:flex-row lg:items-end">
              <div>
                <p className="text-sm font-bold text-[#176B5B]">FUND DISCOVERY</p>
                <h1 className="mt-2 text-3xl font-semibold tracking-tight md:text-4xl">Find the right fund for each role.</h1>
                <p className="mt-2 text-slate-500">Ranked for quality, cost, performance and risk—not just recent returns.</p>
              </div>
              <div className="flex gap-2">
                <button className="rounded-xl border border-slate-200 bg-white px-4 py-3 text-sm font-semibold">Filters · 2</button>
                <button onClick={() => setView("portfolio")} className="rounded-xl bg-[#176B5B] px-4 py-3 text-sm font-semibold text-white">Review my portfolio</button>
              </div>
            </div>

            <section className="mt-8 overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-[0_12px_35px_rgba(25,35,55,.06)]">
              <div className="flex items-center justify-between border-b border-slate-100 px-5 py-4">
                <div><h2 className="font-semibold">Top-ranked funds</h2><p className="text-sm text-slate-500">Large-cap core · 20 eligible funds</p></div>
                <span className="rounded-full bg-emerald-50 px-3 py-1 text-xs font-bold text-[#176B5B]">Updated today</span>
              </div>
              <div className="overflow-x-auto">
                <table className="w-full min-w-[900px] text-left text-sm">
                  <thead className="bg-slate-50 text-xs uppercase tracking-wide text-slate-500">
                    <tr><th className="p-4">Compare</th><th>Fund</th><th>Class</th><th>Score</th><th>Expense</th><th>5Y return</th><th>Risk</th><th></th></tr>
                  </thead>
                  <tbody>
                    {funds.map((fund, index) => (
                      <tr key={fund.ticker} className="border-t border-slate-100 hover:bg-slate-50/70">
                        <td className="p-4"><input aria-label={`Compare ${fund.ticker}`} type="checkbox" checked={selected.includes(fund.ticker)} onChange={() => toggleFund(fund.ticker)} className="h-4 w-4 accent-[#176B5B]" /></td>
                        <td className="py-4"><div className="flex items-center gap-3"><span className="grid h-8 w-8 place-items-center rounded-full bg-slate-100 text-xs font-bold">{index + 1}</span><div><b>{fund.ticker}</b><p className="max-w-[230px] truncate text-xs text-slate-500">{fund.name}</p></div></div></td>
                        <td><span className="rounded-full bg-blue-50 px-2.5 py-1 text-xs font-semibold text-blue-700">{fund.category}</span></td>
                        <td><b className="text-[#176B5B]">{fund.score}</b><span className="text-slate-400"> / 100</span></td>
                        <td>{fund.expense}</td><td className="font-semibold">{fund.returns}</td><td>{fund.risk}</td>
                        <td className="pr-4 text-right"><button onClick={() => setDetailOpen(true)} className="rounded-lg border border-slate-200 px-3 py-2 font-semibold hover:border-[#176B5B] hover:text-[#176B5B]">Quick view</button></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>
          </>
        )}

        {view === "compare" && (
          <>
            <button onClick={() => setView("recommendations")} className="text-sm font-semibold text-slate-500 hover:text-slate-900">← Back to recommendations</button>
            <div className="mt-5 flex flex-col justify-between gap-4 md:flex-row md:items-end">
              <div><p className="text-sm font-bold text-[#176B5B]">COMPARE FUNDS</p><h1 className="mt-2 text-3xl font-semibold">See what you really own twice.</h1><p className="mt-2 text-slate-500">Performance can differ while underlying holdings remain very similar.</p></div>
              <button className="rounded-xl border border-slate-200 bg-white px-4 py-3 text-sm font-semibold">+ Add another fund</button>
            </div>
            <div className="mt-7 grid gap-5 lg:grid-cols-[1fr_1.45fr]">
              <section className="rounded-2xl border border-slate-200 bg-white p-6">
                <p className="text-sm font-bold text-slate-500">HOLDINGS OVERLAP</p>
                <div className="mt-4 h-64">
                  <ResponsiveContainer><PieChart><Pie data={overlapData} dataKey="value" innerRadius={68} outerRadius={98} paddingAngle={2}>{overlapData.map((item) => <Cell key={item.name} fill={item.color} />)}</Pie><Tooltip /></PieChart></ResponsiveContainer>
                </div>
                <div className="-mt-36 mb-24 text-center"><b className="text-4xl">72%</b><p className="text-sm text-slate-500">weighted overlap</p></div>
                <div className="rounded-xl bg-amber-50 p-4 text-sm text-amber-900"><b>High similarity.</b> These funds may not add meaningful diversification together.</div>
              </section>
              <section className="rounded-2xl border border-slate-200 bg-white p-6">
                <div className="grid gap-3 sm:grid-cols-2">
                  {selectedFunds.slice(0, 2).map((fund) => <div key={fund.ticker} className="rounded-xl border border-slate-200 p-4"><div className="flex items-center gap-2"><span className="h-3 w-3 rounded-full" style={{ background: fund.color }} /><b>{fund.ticker}</b></div><p className="mt-1 text-xs text-slate-500">{fund.name}</p></div>)}
                </div>
                <h2 className="mt-6 font-semibold">Key differences</h2>
                <div className="mt-3 overflow-hidden rounded-xl border border-slate-200 text-sm">
                  {[["Top 10 concentration", "34.2%", "28.7%"], ["U.S. equity", "99.4%", "96.1%"], ["Mid + small cap", "0.0%", "15.3%"], ["Expense ratio", "0.015%", "0.015%"]].map((row) => <div key={row[0]} className="grid grid-cols-3 border-b border-slate-100 p-3 last:border-0"><span className="text-slate-500">{row[0]}</span><b>{row[1]}</b><b>{row[2]}</b></div>)}
                </div>
                <h2 className="mt-6 font-semibold">Largest divergent holdings</h2>
                <div className="mt-3 h-48"><ResponsiveContainer><BarChart layout="vertical" data={[{name:"Berkshire",v:1.8},{name:"Eli Lilly",v:1.2},{name:"Broadcom",v:.9},{name:"JPMorgan",v:.7}]}><XAxis type="number" hide /><YAxis dataKey="name" type="category" width={80} axisLine={false} tickLine={false} fontSize={12}/><Bar dataKey="v" fill="#4C67E8" radius={[0,6,6,0]} /></BarChart></ResponsiveContainer></div>
              </section>
            </div>
          </>
        )}

        {view === "portfolio" && (
          <>
            <button onClick={() => setView("recommendations")} className="text-sm font-semibold text-slate-500 hover:text-slate-900">← Back to recommendations</button>
            <div className="mt-5 flex flex-col justify-between gap-4 md:flex-row md:items-end">
              <div><p className="text-sm font-bold text-[#176B5B]">MY PORTFOLIO</p><h1 className="mt-2 text-3xl font-semibold">$121,670 invested</h1><p className="mt-2 text-slate-500">Your allocation, duplication and next best actions.</p></div>
              <button className="rounded-xl bg-[#176B5B] px-4 py-3 text-sm font-semibold text-white">+ Add holding</button>
            </div>
            <div className="mt-7 grid gap-5 lg:grid-cols-[.9fr_1.4fr]">
              <section className="rounded-2xl border border-slate-200 bg-white p-6">
                <h2 className="font-semibold">Current allocation</h2>
                <div className="h-64"><ResponsiveContainer><PieChart><Pie data={portfolioData} dataKey="value" innerRadius={62} outerRadius={96}>{portfolioData.map((item) => <Cell key={item.name} fill={item.color} />)}</Pie><Tooltip /></PieChart></ResponsiveContainer></div>
                <div className="space-y-3">{portfolioData.map((item) => <div key={item.name} className="grid grid-cols-[1fr_auto_auto] gap-4 text-sm"><span className="flex items-center gap-2"><i className="h-2.5 w-2.5 rounded-full" style={{background:item.color}} />{item.name}</span><b>{item.value}%</b><span className="w-20 text-right text-slate-500">{item.amount}</span></div>)}</div>
              </section>
              <section className="rounded-2xl border border-slate-200 bg-white p-6">
                <div className="flex items-center justify-between"><div><p className="text-sm font-bold text-slate-500">RECOMMENDED ACTIONS</p><h2 className="mt-1 text-xl font-semibold">3 opportunities to improve</h2></div><span className="rounded-full bg-amber-50 px-3 py-1 text-xs font-bold text-amber-800">Review suggested</span></div>
                <div className="mt-5 space-y-3">
                  {[
                    ["Reduce overlap", "FXAIX + FSPGX", "68% of FSPGX is already represented in FXAIX. Consider consolidating.", "Review", "bg-amber-50 text-amber-800"],
                    ["Increase diversification", "International equity", "Your 15% allocation is below your selected 25% target.", "Buy $12,200", "bg-emerald-50 text-emerald-800"],
                    ["Rebalance", "U.S. large cap", "This allocation is 9% above target after recent growth.", "Sell $10,950", "bg-blue-50 text-blue-800"]
                  ].map(([type,title,copy,action,color]) => (
                    <div key={title} className="rounded-xl border border-slate-200 p-4">
                      <div className="flex flex-col justify-between gap-3 sm:flex-row sm:items-start">
                        <div><span className={`rounded-full px-2 py-1 text-xs font-bold ${color}`}>{type}</span><h3 className="mt-3 font-semibold">{title}</h3><p className="mt-1 max-w-xl text-sm leading-6 text-slate-500">{copy}</p></div>
                        <button className="shrink-0 rounded-lg border border-slate-200 px-3 py-2 text-sm font-semibold">{action}</button>
                      </div>
                    </div>
                  ))}
                </div>
                <p className="mt-4 text-xs text-slate-400">Suggestions are educational and based on your selected targets—not personalized financial advice.</p>
              </section>
            </div>
          </>
        )}
      </div>

      {view === "recommendations" && selected.length >= 2 && (
        <div className="fixed bottom-5 left-1/2 z-20 flex w-[calc(100%-2rem)] max-w-xl -translate-x-1/2 items-center justify-between rounded-2xl bg-[#172033] px-4 py-3 text-white shadow-2xl">
          <div><b className="text-sm">{selected.length} funds selected</b><p className="text-xs text-slate-300">{selected.join(" · ")}</p></div>
          <button onClick={() => setView("compare")} className="rounded-xl bg-white px-4 py-2 text-sm font-bold text-slate-900">Compare funds →</button>
        </div>
      )}

      {detailOpen && (
        <div className="fixed inset-0 z-40 bg-slate-950/30" onClick={() => setDetailOpen(false)}>
          <aside className="ml-auto h-full w-full max-w-[580px] overflow-y-auto bg-white p-6 shadow-2xl" onClick={(event) => event.stopPropagation()}>
            <div className="flex items-start justify-between"><div><span className="rounded-full bg-blue-50 px-2 py-1 text-xs font-bold text-blue-700">Large Blend</span><h2 className="mt-3 text-2xl font-semibold">FXAIX</h2><p className="text-sm text-slate-500">Fidelity 500 Index Fund</p></div><button aria-label="Close" onClick={() => setDetailOpen(false)} className="grid h-10 w-10 place-items-center rounded-full bg-slate-100 text-xl">×</button></div>
            <div className="mt-6 grid grid-cols-3 gap-3">{[["Score","96 / 100"],["Expense","0.015%"],["5Y return","14.84%"]].map(([a,b]) => <div key={a} className="rounded-xl bg-slate-50 p-3"><p className="text-xs text-slate-500">{a}</p><b className="mt-1 block">{b}</b></div>)}</div>
            <div className="mt-7 flex items-center justify-between"><div><h3 className="font-semibold">Historical NAV</h3><p className="text-xs text-slate-500">Growth of $100 · not a timing signal by itself</p></div><div className="rounded-lg bg-slate-100 p-1 text-xs font-semibold"><span className="rounded-md bg-white px-2 py-1 shadow-sm">5Y</span><span className="px-2">10Y</span></div></div>
            <div className="mt-4 h-64"><ResponsiveContainer><AreaChart data={navData}><defs><linearGradient id="nav" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stopColor="#176B5B" stopOpacity={.25}/><stop offset="1" stopColor="#176B5B" stopOpacity={0}/></linearGradient></defs><CartesianGrid vertical={false} stroke="#E8EBF0"/><XAxis dataKey="year" axisLine={false} tickLine={false} fontSize={11}/><YAxis hide/><Tooltip/><Area type="monotone" dataKey="nav" stroke="#176B5B" strokeWidth={3} fill="url(#nav)" /></AreaChart></ResponsiveContainer></div>
            <div className="rounded-xl border border-emerald-200 bg-emerald-50 p-4"><div className="flex items-center justify-between"><b className="text-emerald-900">Historical context: Neutral</b><span className="rounded-full bg-white px-2 py-1 text-xs font-bold text-emerald-800">58th percentile</span></div><p className="mt-2 text-sm leading-6 text-emerald-900/75">Current valuation and drawdown are near their 5-year median. Consider your time horizon and allocation—not NAV alone.</p></div>
            <div className="mt-5 grid grid-cols-2 gap-3"><button onClick={() => toggleFund("FXAIX")} className="rounded-xl border border-slate-200 py-3 font-semibold">{selected.includes("FXAIX") ? "Remove from compare" : "Add to compare"}</button><button onClick={() => setView("portfolio")} className="rounded-xl bg-[#176B5B] py-3 font-semibold text-white">Add to portfolio</button></div>
          </aside>
        </div>
      )}
    </main>
  );
}
