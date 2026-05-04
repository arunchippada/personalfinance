import argparse
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
data_folder = PROJECT_ROOT / 'data'

CLASS_TAXONOMY = [
    ('US Large Cap Core', 'Main US equity anchor'),
    ('US Large Cap Growth', 'Optional growth tilt'),
    ('US Large Cap Value', 'Optional value tilt'),
    ('US Mid Cap', 'US diversification beyond large cap'),
    ('US Small Cap', 'Higher-risk US diversification'),
    ('International Developed Large Cap', 'Main international equity'),
    ('International Developed Small/Mid', 'Optional international diversification'),
    ('Emerging Markets Equity', 'Higher-risk international growth'),
    ('Core Bond', 'Main bond anchor'),
    ('Short-Term Bond', 'Lower-duration stabilizer'),
    ('Inflation-Protected Bond / TIPS', 'Inflation hedge'),
    ('High Yield / Credit Bond', 'Optional riskier income'),
    ('Global / International Bond', 'Optional bond diversification'),
    ('Real Estate / REIT', 'Optional diversifier'),
    ('Sector / Thematic Equity', 'Usually excluded or satellite only'),
    ('Allocation / Target Date / Multi-Asset', 'Usually excluded from custom portfolio'),
    ('Alternatives / Trading / Commodities', 'Usually excluded or advanced only'),
    ('Municipal Bonds', 'Usually exclude for retirement account'),
    ('Other', 'Could not be classified from Morningstar Category'),
]

EXACT_CATEGORY_TO_CLASS = {
    'Large Blend': 'US Large Cap Core',
    'Large Growth': 'US Large Cap Growth',
    'Large Value': 'US Large Cap Value',
    'Mid-Cap Blend': 'US Mid Cap',
    'Mid-Cap Growth': 'US Mid Cap',
    'Mid-Cap Value': 'US Mid Cap',
    'Small Blend': 'US Small Cap',
    'Small Growth': 'US Small Cap',
    'Small Value': 'US Small Cap',
    'Foreign Large Blend': 'International Developed Large Cap',
    'Foreign Large Growth': 'International Developed Large Cap',
    'Foreign Large Value': 'International Developed Large Cap',
    'Foreign Small/Mid Blend': 'International Developed Small/Mid',
    'Foreign Small/Mid Growth': 'International Developed Small/Mid',
    'Foreign Small/Mid Value': 'International Developed Small/Mid',
    'Europe Stock': 'International Developed Large Cap',
    'Japan Stock': 'International Developed Large Cap',
    'Diversified Pacific/Asia': 'International Developed Large Cap',
    'Pacific/Asia ex-Japan Stk': 'International Developed Large Cap',
    'Global Large-Stock Blend': 'International Developed Large Cap',
    'Global Large-Stock Growth': 'International Developed Large Cap',
    'Global Large-Stock Value': 'International Developed Large Cap',
    'Global Small/Mid Stock': 'International Developed Small/Mid',
    'Diversified Emerging Mkts': 'Emerging Markets Equity',
    'Greater China Region': 'Emerging Markets Equity',
    'India Equity': 'Emerging Markets Equity',
    'Latin America Stock': 'Emerging Markets Equity',
    'Intermediate Core Bond': 'Core Bond',
    'Intermediate Core-Plus Bond': 'Core Bond',
    'Intermediate Government': 'Core Bond',
    'Government Mortgage-Backed Bond': 'Core Bond',
    'Long Government': 'Core Bond',
    'Long-Term Bond': 'Core Bond',
    'Corporate Bond': 'Core Bond',
    'Securitized Bond - Diversified': 'Core Bond',
    'Securitized Bond - Focused': 'Core Bond',
    'Short-Term Bond': 'Short-Term Bond',
    'Ultrashort Bond': 'Short-Term Bond',
    'Short Government': 'Short-Term Bond',
    'Inflation-Protected Bond': 'Inflation-Protected Bond / TIPS',
    'Short-Term Inflation-Protected Bond': 'Inflation-Protected Bond / TIPS',
    'High Yield Bond': 'High Yield / Credit Bond',
    'Bank Loan': 'High Yield / Credit Bond',
    'Convertibles': 'High Yield / Credit Bond',
    'Multisector Bond': 'High Yield / Credit Bond',
    'Nontraditional Bond': 'High Yield / Credit Bond',
    'Preferred Stock': 'High Yield / Credit Bond',
    'Emerging Markets Bond': 'Global / International Bond',
    'Emerging-Markets Local-Currency Bond': 'Global / International Bond',
    'Global Bond': 'Global / International Bond',
    'Global Bond-USD Hedged': 'Global / International Bond',
    'World Bond': 'Global / International Bond',
    'Real Estate': 'Real Estate / REIT',
    'Global Real Estate': 'Real Estate / REIT',
    'Technology': 'Sector / Thematic Equity',
    'Health': 'Sector / Thematic Equity',
    'Financial': 'Sector / Thematic Equity',
    'Utilities': 'Sector / Thematic Equity',
    'Communications': 'Sector / Thematic Equity',
    'Consumer Cyclical': 'Sector / Thematic Equity',
    'Consumer Defensive': 'Sector / Thematic Equity',
    'Equity Energy': 'Sector / Thematic Equity',
    'Energy Limited Partnership': 'Sector / Thematic Equity',
    'Equity Precious Metals': 'Sector / Thematic Equity',
    'Focused Region': 'Sector / Thematic Equity',
    'Industrials': 'Sector / Thematic Equity',
    'Infrastructure': 'Sector / Thematic Equity',
    'Miscellaneous Sector': 'Sector / Thematic Equity',
    'Natural Resources': 'Sector / Thematic Equity',
    'Aggressive Allocation': 'Allocation / Target Date / Multi-Asset',
    'Conservative Allocation': 'Allocation / Target Date / Multi-Asset',
    'Global Aggressive Allocation': 'Allocation / Target Date / Multi-Asset',
    'Global Conservative Allocation': 'Allocation / Target Date / Multi-Asset',
    'Global Moderate Allocation': 'Allocation / Target Date / Multi-Asset',
    'Global Moderately Aggressive Allocation': 'Allocation / Target Date / Multi-Asset',
    'Global Moderately Conservative Allocation': 'Allocation / Target Date / Multi-Asset',
    'Miscellaneous Allocation': 'Allocation / Target Date / Multi-Asset',
    'Moderate Allocation': 'Allocation / Target Date / Multi-Asset',
    'Moderately Aggressive Allocation': 'Allocation / Target Date / Multi-Asset',
    'Moderately Conservative Allocation': 'Allocation / Target Date / Multi-Asset',
    'Multi-Asset Overlay': 'Allocation / Target Date / Multi-Asset',
    'Tactical Allocation': 'Allocation / Target Date / Multi-Asset',
    'Commodities Broad Basket': 'Alternatives / Trading / Commodities',
    'Commodities Focused': 'Alternatives / Trading / Commodities',
    'Derivative Income': 'Alternatives / Trading / Commodities',
    'Equity Hedged': 'Alternatives / Trading / Commodities',
    'Equity Market Neutral': 'Alternatives / Trading / Commodities',
    'Event Driven': 'Alternatives / Trading / Commodities',
    'Long-Short Equity': 'Alternatives / Trading / Commodities',
    'Macro Trading': 'Alternatives / Trading / Commodities',
    'Multistrategy': 'Alternatives / Trading / Commodities',
    'Relative Value Arbitrage': 'Alternatives / Trading / Commodities',
    'Single Currency': 'Alternatives / Trading / Commodities',
    'Systematic Trend': 'Alternatives / Trading / Commodities',
    'Trading--Inverse Equity': 'Alternatives / Trading / Commodities',
    'Trading--Leveraged Equity': 'Alternatives / Trading / Commodities',
    'Trading--Miscellaneous': 'Alternatives / Trading / Commodities',
    'High Yield Muni': 'Municipal Bonds',
    'Muni California Intermediate': 'Municipal Bonds',
    'Muni California Long': 'Municipal Bonds',
    'Muni Massachusetts': 'Municipal Bonds',
    'Muni Minnesota': 'Municipal Bonds',
    'Muni National Interm': 'Municipal Bonds',
    'Muni National Long': 'Municipal Bonds',
    'Muni National Short': 'Municipal Bonds',
    'Muni New Jersey': 'Municipal Bonds',
    'Muni New York Intermediate': 'Municipal Bonds',
    'Muni New York Long': 'Municipal Bonds',
    'Muni Ohio': 'Municipal Bonds',
    'Muni Pennsylvania': 'Municipal Bonds',
    'Muni Single State Interm': 'Municipal Bonds',
    'Muni Single State Long': 'Municipal Bonds',
    'Muni Single State Short': 'Municipal Bonds',
}

TAXONOMY_BY_CLASS = dict(CLASS_TAXONOMY)

def classify_morningstar_category(category):
    if pd.isna(category):
        return 'Other'

    category = str(category).strip()

    if category in EXACT_CATEGORY_TO_CLASS:
        return EXACT_CATEGORY_TO_CLASS[category]

    if category.startswith('Target-Date'):
        return 'Allocation / Target Date / Multi-Asset'

    if category.startswith('Muni '):
        return 'Municipal Bonds'

    if 'Allocation' in category:
        return 'Allocation / Target Date / Multi-Asset'

    return 'Other'

def write_class_taxonomy(folder):
    taxonomy_file = folder / 'fund_class_taxonomy.csv'
    taxonomy_df = pd.DataFrame(CLASS_TAXONOMY, columns=['Fund Class', 'Use in Portfolio'])
    taxonomy_df.to_csv(taxonomy_file, index=False)
    return taxonomy_file

def classify_and_score_funds(folder=data_folder):
    folder = Path(folder)
    input_file = folder / 'fidelity_funds_data_cleaned.csv'
    enriched_file = folder / 'fidelity_funds_data_enriched.csv'
    results_file = folder / 'classification_results.csv'

    df = pd.read_csv(input_file)
    df['Fund Class'] = df['Morningstar Category'].apply(classify_morningstar_category)
    df['Fund Class Use'] = df['Fund Class'].map(TAXONOMY_BY_CLASS)

    df.to_csv(enriched_file, index=False)

    results_df = (
        df['Fund Class']
        .value_counts()
        .rename_axis('Fund Class')
        .reset_index(name='Fund Count')
        .sort_values(['Fund Count', 'Fund Class'], ascending=[False, True])
    )
    results_df.to_csv(results_file, index=False)
    taxonomy_file = write_class_taxonomy(folder)

    print('Classification summary')
    print(f'Total input rows: {len(df)}')
    print(f'Enriched data written to {enriched_file}')
    print(f'Classification results written to {results_file}')
    print(f'Class taxonomy written to {taxonomy_file}')

    return enriched_file, results_file

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Classify and score Fidelity fund data.')
    parser.add_argument(
        'folder',
        nargs='?',
        default=data_folder,
        help='Folder containing fidelity_funds_data_cleaned.csv',
    )
    args = parser.parse_args()
    classify_and_score_funds(args.folder)
