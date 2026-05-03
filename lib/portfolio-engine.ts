import type {
  BuilderConstraints,
  FundRecord,
  FundRole,
  PortfolioFundSelection,
  PortfolioRecommendation,
  ScoredFund,
  WeightedMetrics
} from "@/lib/types";

const ROLE_ORDER: FundRole[] = [
  "US Large Cap",
  "US Extended Market / Mid-Small Cap",
  "International Developed Equity",
  "Emerging Markets Equity",
  "Sector / Thematic Equity",
  "Core Bond",
  "Short-Term Bond",
  "TIPS / Inflation-Protected Bond",
  "Other / Excluded"
];

type ScoreRange = {
  min: number;
  max: number;
};

type RoleInsights = {
  expenseRange?: ScoreRange;
  longTermRange?: ScoreRange;
  fiveYearRange?: ScoreRange;
  riskRange?: ScoreRange;
  ratingRange?: ScoreRange;
  cheapestFundId?: string;
  strongestFiveYearFundId?: string;
  lowestRiskFundId?: string;
};

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

function getScoreRange(values: number[]): ScoreRange | undefined {
  if (values.length === 0) return undefined;

  let min = values[0];
  let max = values[0];

  for (let index = 1; index < values.length; index += 1) {
    const value = values[index];
    if (value < min) min = value;
    if (value > max) max = value;
  }

  return { min, max };
}

function normalizeScore(value: number | undefined, range: ScoreRange | undefined, lowerIsBetter = false): number | undefined {
  if (value === undefined || !range) return undefined;
  if (range.min === range.max) return 100;
  const ratio = lowerIsBetter
    ? (range.max - value) / (range.max - range.min)
    : (value - range.min) / (range.max - range.min);
  return clamp(ratio * 100, 0, 100);
}

function getRoleCandidates(funds: FundRecord[], role: FundRole, constraints: BuilderConstraints): FundRecord[] {
  return funds.filter((fund) => {
    if (fund.role !== role) return false;
    if (constraints.excludeSectorThematic && role === "Sector / Thematic Equity") return false;
    if (fund.expenseRatio !== undefined && fund.expenseRatio > constraints.maxExpenseRatio) return false;
    return true;
  });
}

function getRoleInsights(roleFunds: FundRecord[]): RoleInsights {
  const expenseValues: number[] = [];
  const longTermValues: number[] = [];
  const fiveYearValues: number[] = [];
  const riskValues: number[] = [];
  const ratingValues: number[] = [];

  let cheapestFundId: string | undefined;
  let cheapestExpense = Number.POSITIVE_INFINITY;
  let strongestFiveYearFundId: string | undefined;
  let strongestFiveYear = Number.NEGATIVE_INFINITY;
  let lowestRiskFundId: string | undefined;
  let lowestRisk = Number.POSITIVE_INFINITY;

  for (const fund of roleFunds) {
    if (fund.expenseRatio !== undefined) {
      expenseValues.push(fund.expenseRatio);
      if (fund.expenseRatio < cheapestExpense) {
        cheapestExpense = fund.expenseRatio;
        cheapestFundId = fund.id;
      }
    }

    const longTermMetric = fund.returns["10Y"] ?? fund.returns.Life ?? fund.returns["3Y"];
    if (longTermMetric !== undefined) {
      longTermValues.push(longTermMetric);
    }

    const fiveYearReturn = fund.returns["5Y"];
    if (fiveYearReturn !== undefined) {
      fiveYearValues.push(fiveYearReturn);
      if (fiveYearReturn > strongestFiveYear) {
        strongestFiveYear = fiveYearReturn;
        strongestFiveYearFundId = fund.id;
      }
    }

    const riskMetric = fund.standardDeviation ?? fund.riskCategory;
    if (riskMetric !== undefined) {
      riskValues.push(riskMetric);
      if (riskMetric < lowestRisk) {
        lowestRisk = riskMetric;
        lowestRiskFundId = fund.id;
      }
    }

    if (fund.morningstarRating !== undefined) {
      ratingValues.push(fund.morningstarRating);
    }
  }

  return {
    expenseRange: getScoreRange(expenseValues),
    longTermRange: getScoreRange(longTermValues),
    fiveYearRange: getScoreRange(fiveYearValues),
    riskRange: getScoreRange(riskValues),
    ratingRange: getScoreRange(ratingValues),
    cheapestFundId,
    strongestFiveYearFundId,
    lowestRiskFundId
  };
}

function buildSelectionReasons(
  fund: FundRecord,
  insights: RoleInsights,
  scoreBreakdown: ScoredFund["scoreBreakdown"]
): string[] {
  const reasons: string[] = [];

  if (insights.cheapestFundId === fund.id) {
    reasons.push("Selected as the lowest-cost strong candidate in this role.");
  }

  if (insights.strongestFiveYearFundId === fund.id) {
    reasons.push("Selected because it has one of the strongest available 5-year return profiles in this role.");
  }

  if (insights.lowestRiskFundId === fund.id) {
    reasons.push("Selected in part because it helps reduce volatility relative to peers in this role.");
  }

  if (fund.role === "US Large Cap") {
    reasons.push("Selected to provide broad US equity exposure.");
  } else if (fund.role === "US Extended Market / Mid-Small Cap") {
    reasons.push("Selected to complement large-cap exposure with smaller-company diversification.");
  } else if (fund.role === "International Developed Equity") {
    reasons.push("Selected to improve international diversification outside the US.");
  } else if (fund.role === "Emerging Markets Equity") {
    reasons.push("Selected to add emerging markets exposure in a limited supporting role.");
  } else if (fund.role === "Core Bond") {
    reasons.push("Selected to stabilize the portfolio with broad bond exposure.");
  } else if (fund.role === "Short-Term Bond") {
    reasons.push("Selected to help dampen interest-rate sensitivity and support liquidity.");
  } else if (fund.role === "TIPS / Inflation-Protected Bond") {
    reasons.push("Selected to add explicit inflation-sensitive bond exposure.");
  } else if (fund.role === "Sector / Thematic Equity") {
    reasons.push("Included only as a modest satellite position because concentrated themes add risk.");
  }

  if ((fund.expenseRatio ?? 0) >= 0.9) {
    reasons.push("This fund scores well enough on other dimensions to offset its higher fee burden.");
  }

  if (scoreBreakdown.some((item) => item.label === "Risk" && item.value >= 80)) {
    reasons.push("Selected despite lower return tradeoffs because its risk profile is comparatively steadier.");
  }

  return Array.from(new Set(reasons)).slice(0, 4);
}

// Fund scoring stays deterministic and transparent: all scores come from simple normalized metrics by role.
export function scoreFundsByRole(
  funds: FundRecord[],
  constraints: BuilderConstraints
): Record<FundRole, ScoredFund[]> {
  const roleScores = {} as Record<FundRole, ScoredFund[]>;

  for (const role of ROLE_ORDER) {
    const roleFunds = getRoleCandidates(funds, role, constraints);
    const insights = getRoleInsights(roleFunds);

    roleScores[role] = roleFunds
      .map((fund) => {
        const weights = {
          expense: constraints.lowCostPriority ? 42 : 30,
          longTerm: 25,
          fiveYear: 20,
          risk: 15,
          rating: 10
        };

        const expenseComponent = normalizeScore(fund.expenseRatio, insights.expenseRange, true);
        const longTermMetric = fund.returns["10Y"] ?? fund.returns.Life ?? fund.returns["3Y"];
        const longTermComponent = normalizeScore(longTermMetric, insights.longTermRange, false);
        const fiveYearComponent = normalizeScore(fund.returns["5Y"], insights.fiveYearRange, false);
        const riskMetric = fund.standardDeviation ?? fund.riskCategory;
        const riskComponent = normalizeScore(riskMetric, insights.riskRange, true);
        const ratingComponent = normalizeScore(fund.morningstarRating, insights.ratingRange, false);

        const scoreBreakdown = [
          {
            label: "Expense",
            value: expenseComponent ?? 0,
            detail:
              fund.expenseRatio !== undefined
                ? `${fund.expenseRatio.toFixed(2)}% net expense ratio`
                : "Missing expense ratio"
          },
          {
            label: "Long-Term Return",
            value: longTermComponent ?? 0,
            detail:
              longTermMetric !== undefined
                ? `${longTermMetric.toFixed(2)}% long-term return proxy`
                : "Missing long-term return metric"
          },
          {
            label: "5Y Return",
            value: fiveYearComponent ?? 0,
            detail:
              fund.returns["5Y"] !== undefined
                ? `${fund.returns["5Y"]?.toFixed(2)}% 5-year return`
                : "Missing 5-year return"
          },
          {
            label: "Risk",
            value: riskComponent ?? 0,
            detail:
              riskMetric !== undefined ? `${riskMetric.toFixed(2)} risk metric` : "Missing risk metric"
          },
          {
            label: "Rating",
            value: ratingComponent ?? 0,
            detail:
              fund.morningstarRating !== undefined
                ? `${fund.morningstarRating.toFixed(1)} Morningstar rating`
                : "Missing rating"
          }
        ];

        const weightedParts = [
          expenseComponent !== undefined ? expenseComponent * weights.expense : undefined,
          longTermComponent !== undefined ? longTermComponent * weights.longTerm : undefined,
          fiveYearComponent !== undefined ? fiveYearComponent * weights.fiveYear : undefined,
          riskComponent !== undefined ? riskComponent * weights.risk : undefined,
          ratingComponent !== undefined ? ratingComponent * weights.rating : undefined
        ].filter((value): value is number => value !== undefined);

        const availableWeight =
          (expenseComponent !== undefined ? weights.expense : 0) +
          (longTermComponent !== undefined ? weights.longTerm : 0) +
          (fiveYearComponent !== undefined ? weights.fiveYear : 0) +
          (riskComponent !== undefined ? weights.risk : 0) +
          (ratingComponent !== undefined ? weights.rating : 0);

        let score = availableWeight > 0 ? weightedParts.reduce((sum, part) => sum + part, 0) / availableWeight : 0;

        if (fund.role === "Sector / Thematic Equity") score -= 8;
        if ((fund.expenseRatio ?? 0) > 1.0) score -= 12;
        else if ((fund.expenseRatio ?? 0) > 0.75) score -= 6;

        const scoredFund: ScoredFund = {
          ...fund,
          score: clamp(score, 0, 100),
          scoreBreakdown,
          selectionReasons: []
        };

        scoredFund.selectionReasons = buildSelectionReasons(scoredFund, insights, scoreBreakdown);
        return scoredFund;
      })
      .sort((a, b) => b.score - a.score || (a.expenseRatio ?? Infinity) - (b.expenseRatio ?? Infinity));
  }

  return roleScores;
}

function roundAllocation(value: number): number {
  return Math.round(value * 10) / 10;
}

function rebalanceAllocations(items: Array<{ role: FundRole; allocation: number }>) {
  const total = items.reduce((sum, item) => sum + item.allocation, 0);
  if (total === 100) return items;
  const difference = roundAllocation(100 - total);
  if (items.length === 0) return items;
  const updated = [...items];
  updated[0] = { ...updated[0], allocation: roundAllocation(updated[0].allocation + difference) };
  return updated;
}

function getPortfolioTemplates(stockAllocation: number, targetFunds: number, includeSector: boolean) {
  const bondAllocation = 100 - stockAllocation;
  const simpleStockRoles = [
    { role: "US Large Cap" as FundRole, share: 0.58 },
    { role: "International Developed Equity" as FundRole, share: 0.22 }
  ];
  const simpleBondRoles = [
    { role: "Core Bond" as FundRole, share: 0.62 },
    { role: "Short-Term Bond" as FundRole, share: 0.38 }
  ];

  const balancedStockRoles = [
    { role: "US Large Cap" as FundRole, share: 0.42 },
    { role: "US Extended Market / Mid-Small Cap" as FundRole, share: 0.18 },
    { role: "International Developed Equity" as FundRole, share: 0.26 },
    { role: "Emerging Markets Equity" as FundRole, share: 0.14 }
  ];
  const balancedBondRoles = [
    { role: "Core Bond" as FundRole, share: 0.62 },
    { role: "Short-Term Bond" as FundRole, share: 0.23 },
    { role: "TIPS / Inflation-Protected Bond" as FundRole, share: 0.15 }
  ];

  const granularStockRoles = [
    { role: "US Large Cap" as FundRole, share: includeSector ? 0.35 : 0.39 },
    { role: "US Extended Market / Mid-Small Cap" as FundRole, share: 0.17 },
    { role: "International Developed Equity" as FundRole, share: 0.24 },
    { role: "Emerging Markets Equity" as FundRole, share: 0.12 },
    ...(includeSector ? [{ role: "Sector / Thematic Equity" as FundRole, share: 0.12 }] : [])
  ];
  const granularBondRoles = [
    { role: "Core Bond" as FundRole, share: 0.5 },
    { role: "Short-Term Bond" as FundRole, share: 0.25 },
    { role: "TIPS / Inflation-Protected Bond" as FundRole, share: 0.15 },
    { role: "Core Bond" as FundRole, share: 0.1 }
  ];

  return {
    simple: {
      name: "Simple",
      description: `${clamp(targetFunds - 2, 3, 4)}-fund core portfolio with broad stock and bond coverage.`,
      roleTargets: rebalanceAllocations([
        ...simpleStockRoles.map((item) => ({ role: item.role, allocation: roundAllocation(stockAllocation * item.share) })),
        ...simpleBondRoles.map((item) => ({ role: item.role, allocation: roundAllocation(bondAllocation * item.share) }))
      ])
    },
    balanced: {
      name: "Balanced",
      description: `${clamp(targetFunds, 5, 7)}-fund portfolio that adds small-cap, EM, and inflation-sensitive bonds.`,
      roleTargets: rebalanceAllocations([
        ...balancedStockRoles.map((item) => ({ role: item.role, allocation: roundAllocation(stockAllocation * item.share) })),
        ...balancedBondRoles.map((item) => ({ role: item.role, allocation: roundAllocation(bondAllocation * item.share) }))
      ])
    },
    granular: {
      name: "Granular",
      description: `${clamp(targetFunds + 2, 8, 10)}-fund version with more explicit role separation and smaller sleeves.`,
      roleTargets: rebalanceAllocations([
        ...granularStockRoles.map((item) => ({ role: item.role, allocation: roundAllocation(stockAllocation * item.share) })),
        ...granularBondRoles.map((item) => ({ role: item.role, allocation: roundAllocation(bondAllocation * item.share) }))
      ])
    }
  };
}

function weightedAverage(
  selections: PortfolioFundSelection[],
  getter: (selection: PortfolioFundSelection) => number | undefined
) {
  let numerator = 0;
  let denominator = 0;

  for (const selection of selections) {
    const value = getter(selection);
    if (value === undefined) continue;
    numerator += value * selection.allocation;
    denominator += selection.allocation;
  }

  return denominator > 0 ? numerator / denominator : undefined;
}

function summarizeRisk(
  stockAllocation: number,
  weightedStdDev: number | undefined,
  selectedFunds: PortfolioFundSelection[]
) {
  const hasThematic = selectedFunds.some((selection) => selection.role === "Sector / Thematic Equity");
  if (weightedStdDev === undefined) {
    return "Risk summary is partial because standard deviation data is missing for some selected funds.";
  }

  if (stockAllocation >= 75 || weightedStdDev >= 15) {
    return hasThematic
      ? "Higher-risk mix: heavier stock exposure plus a thematic sleeve can make results more uneven."
      : "Higher-risk mix: the larger stock weight is likely to drive wider swings over time.";
  }

  if (stockAllocation <= 45 || weightedStdDev <= 8) {
    return "Lower-volatility mix: bond exposure is doing more of the stabilizing work here.";
  }

  return "Moderate risk mix: diversified equities drive growth while the bond sleeves aim to dampen drawdowns.";
}

// Explanations are deterministic text templates so the reasoning is inspectable and stable across runs.
export function buildFundExplanation(
  fund: ScoredFund,
  role: FundRole,
  constraints: BuilderConstraints
): string[] {
  const bullets: string[] = [];
  const alreadyHighlightsLowCost = fund.selectionReasons.includes(
    "Selected as the lowest-cost strong candidate in this role."
  );

  if (!alreadyHighlightsLowCost && constraints.lowCostPriority && fund.expenseRatio !== undefined) {
    bullets.push("Selected with a cost-aware tilt, even though it is not the absolute cheapest option.");
  }

  bullets.push(...fund.selectionReasons);

  if (role === "Sector / Thematic Equity" && constraints.excludeSectorThematic) {
    bullets.push("Sector and thematic funds were excluded to avoid concentration risk.");
  }

  return Array.from(new Set(bullets)).slice(0, 4);
}

// Portfolio generation uses small role templates instead of optimization so every pick is traceable.
export function buildPortfolioRecommendations(
  funds: FundRecord[],
  constraints: BuilderConstraints
): {
  recommendations: PortfolioRecommendation[];
  rankedCandidates: Record<FundRole, ScoredFund[]>;
} {
  const rankedCandidates = scoreFundsByRole(funds, constraints);
  const templates = getPortfolioTemplates(
    constraints.stockAllocation,
    constraints.fundTarget,
    !constraints.excludeSectorThematic
  );

  const recommendations = (["simple", "balanced", "granular"] as const).map((key) => {
    const template = templates[key];
    const roleUseCount = new Map<FundRole, number>();

    const selectedFunds = template.roleTargets
      .map((target) => {
        const ranked = rankedCandidates[target.role] ?? [];
        const useIndex = roleUseCount.get(target.role) ?? 0;
        roleUseCount.set(target.role, useIndex + 1);
        const fund = ranked[useIndex] ?? ranked[0];
        if (!fund) return null;

        return {
          fund,
          role: target.role,
          allocation: target.allocation,
          explanation: buildFundExplanation(fund, target.role, constraints)
        } satisfies PortfolioFundSelection;
      })
      .filter((selection): selection is PortfolioFundSelection => Boolean(selection));

    const roleAllocationMap = new Map<FundRole, number>();
    for (const selection of selectedFunds) {
      roleAllocationMap.set(selection.role, (roleAllocationMap.get(selection.role) ?? 0) + selection.allocation);
    }

    const weightedMetrics: WeightedMetrics = {
      expenseRatio: weightedAverage(selectedFunds, (selection) => selection.fund.expenseRatio),
      return1Y: weightedAverage(selectedFunds, (selection) => selection.fund.returns["1Y"]),
      return3Y: weightedAverage(selectedFunds, (selection) => selection.fund.returns["3Y"]),
      return5Y: weightedAverage(selectedFunds, (selection) => selection.fund.returns["5Y"]),
      return10Y: weightedAverage(selectedFunds, (selection) => selection.fund.returns["10Y"]),
      standardDeviation: weightedAverage(selectedFunds, (selection) => selection.fund.standardDeviation)
    };

    const tradeoffs: string[] = [];
    if (constraints.excludeSectorThematic) {
      tradeoffs.push("Sector and thematic funds are excluded, which lowers concentration risk but may miss narrow rallies.");
    } else if (selectedFunds.some((selection) => selection.role === "Sector / Thematic Equity")) {
      tradeoffs.push("The thematic sleeve is intentionally small because concentrated bets can raise volatility.");
    }
    if ((weightedMetrics.expenseRatio ?? 0) > 0.45) {
      tradeoffs.push("This mix accepts somewhat higher fees to retain stronger candidates in weaker categories.");
    } else {
      tradeoffs.push("This mix keeps the weighted expense ratio relatively contained for the selected roles.");
    }
    if ((weightedMetrics.standardDeviation ?? 0) > 14) {
      tradeoffs.push("Historical volatility is on the higher side for the selected mix, mainly from equity-heavy sleeves.");
    } else {
      tradeoffs.push("Bond sleeves help moderate volatility relative to an all-equity approach.");
    }

    return {
      key,
      name: template.name,
      description: template.description,
      selectedFunds,
      stockAllocation: constraints.stockAllocation,
      bondAllocation: 100 - constraints.stockAllocation,
      weightedMetrics,
      roleAllocation: Array.from(roleAllocationMap.entries()).map(([role, allocation]) => ({
        role,
        allocation: roundAllocation(allocation)
      })),
      riskSummary: summarizeRisk(constraints.stockAllocation, weightedMetrics.standardDeviation, selectedFunds),
      tradeoffs
    } satisfies PortfolioRecommendation;
  });

  return { recommendations, rankedCandidates };
}
