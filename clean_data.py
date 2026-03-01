import pandas as pd
import re

data_folder = 'data'
file_name = f'{data_folder}/fidelity_funds_data.csv'

def clean_blank(x):
    if pd.isna(x):
        return None
    if x == '-':
        return None
    return x

def clean_percent(x):
    match = re.search(r'([-+]?\d*\.?\d+)%', str(x))
    return float(match.group(1)) if match else x

def clean_basic(x):
    x = clean_blank(x)
    x = clean_percent(x)
    return x

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

if __name__ == "__main__":
    df = pd.read_csv(file_name)
    for col in df.columns:
        df[col] = df[col].apply(clean_basic)
    for col in ['Expense Ratio - Net', 'Expense Ratio - Gross', 'YTD (Daily)']:
        df[col] = df[col].apply(to_percent)
    df = sep_rating_count(df, ['Morningstar- Overall', 'Morningstar- 3yrs', 'Morningstar- 5yrs', 'Morningstar- 10yrs'])
    df.to_csv(f'{data_folder}/fidelity_funds_data_cleaned.csv', index=False)
