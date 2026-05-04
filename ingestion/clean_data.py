import pandas as pd
import re
import argparse
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
data_folder = PROJECT_ROOT / 'data'

FILTER_RULES = {
    'missing_ticker': 'Name is not in "Fund Name (TICKER)" format',
    'missing_10_year_return': '10 Yr return is missing',
    'missing_net_expense_ratio': 'Expense Ratio - Net is missing',
    'missing_standard_deviation': 'Standard Deviation is missing',
    'missing_3_year_sharpe_ratio': '3 Year Sharpe Ratio is missing',
}

def clean_blank(x):
    if pd.isna(x):
        return None
    if str(x).strip() in ('', '-'):
        return None
    return x

def clean_percent(x):
    match = re.search(r'([-+]?\d*\.?\d+)%', str(x))
    return float(match.group(1)) if match else x

def clean_basic(x):
    x = clean_blank(x)
    x = clean_percent(x)
    return x

def is_missing(x):
    if pd.isna(x):
        return True
    return str(x).strip() in ('', '-')

def extract_name_ticker(x):
    if is_missing(x):
        return pd.Series([None, None])
    match = re.match(r'^\s*(.+?)\s+\(([A-Z0-9.]+)\)\s*$', str(x))
    return pd.Series([match.group(1).strip(), match.group(2)]) if match else pd.Series([None, None])

def extract_number_with_optional_date(x):
    if is_missing(x):
        return None
    match = re.search(r'([-+]?\d*\.?\d+)', str(x))
    return float(match.group(1)) if match else None

def to_percent(x):
    if x is None:
        return None
    return round(100 * float(x), 2)

def extract_rating_count(x):
    if x is None:
        return pd.Series([None, None])
    match = re.search(r'([1-5]) \((\d+) Rated\)', str(x))
    return pd.Series([int(match.group(1)), int(match.group(2))]) if match else pd.Series([None, None])

def sep_rating_count(df, cols):
    for col_name in cols:
        df[[col_name, f'{col_name} Count']] = df[col_name].apply(extract_rating_count)
        df[col_name] = df[col_name].astype('Int64')
        df[f'{col_name} Count'] = df[f'{col_name} Count'].astype('Int64')
    return df

def filter_missing(df, col_name, rule_name, filter_counts):
    keep_mask = ~df[col_name].apply(is_missing)
    filter_counts[rule_name] = int((~keep_mask).sum())
    return df.loc[keep_mask].copy()

def move_ticker_after_name(df):
    cols = list(df.columns)
    cols.remove('Ticker')
    name_index = cols.index('Name')
    cols.insert(name_index + 1, 'Ticker')
    return df[cols]

def write_summary(folder, total_input_rows, output_rows, filter_counts):
    total_filtered_rows = sum(filter_counts.values())
    summary = {
        'total_input_rows': total_input_rows,
        'output_rows': output_rows,
        'rows_filtered_out': total_filtered_rows,
    }
    summary.update(filter_counts)

    summary_file = f'{folder}/fidelity_cleaning_summary.csv'
    pd.DataFrame([summary]).to_csv(summary_file, index=False)

    print('Cleaning summary')
    print(f'Total input rows: {total_input_rows}')
    print(f'Output rows: {output_rows}')
    print(f'Rows filtered out: {total_filtered_rows}')
    for rule_name, count in filter_counts.items():
        print(f'- {rule_name}: {count} ({FILTER_RULES[rule_name]})')
    print(f'Summary written to {summary_file}')

    return summary_file

def clean_data(folder=data_folder):
    file_name = f'{folder}/fidelity_funds_data.csv'
    df = pd.read_csv(file_name)
    total_input_rows = len(df)
    filter_counts = {rule_name: 0 for rule_name in FILTER_RULES}

    for col in df.columns:
        df[col] = df[col].apply(clean_basic)

    df[['Name', 'Ticker']] = df['Name'].apply(extract_name_ticker)
    df = filter_missing(df, 'Ticker', 'missing_ticker', filter_counts)

    for col in ['Expense Ratio - Net', 'Expense Ratio - Gross', 'YTD (Daily)']:
        df[col] = df[col].apply(to_percent)

    df['Standard Deviation'] = df['Standard Deviation'].apply(extract_number_with_optional_date)
    df['3 Year Sharpe Ratio'] = df['3 Year Sharpe Ratio'].apply(extract_number_with_optional_date)

    df = filter_missing(df, '10 Yr', 'missing_10_year_return', filter_counts)
    df = filter_missing(df, 'Expense Ratio - Net', 'missing_net_expense_ratio', filter_counts)
    df = filter_missing(df, 'Standard Deviation', 'missing_standard_deviation', filter_counts)
    df = filter_missing(df, '3 Year Sharpe Ratio', 'missing_3_year_sharpe_ratio', filter_counts)
    df = move_ticker_after_name(df)

    df = sep_rating_count(df, ['Morningstar- Overall', 'Morningstar- 3yrs', 'Morningstar- 5yrs', 'Morningstar- 10yrs'])
    output_file = f'{folder}/fidelity_funds_data_cleaned.csv'
    df.to_csv(output_file, index=False)
    write_summary(folder, total_input_rows, len(df), filter_counts)
    return output_file

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clean Fidelity mutual fund data.")
    parser.add_argument(
        "folder",
        nargs="?",
        default=data_folder,
        help="Folder containing fidelity_funds_data.csv",
    )
    args = parser.parse_args()
    clean_data(args.folder)
