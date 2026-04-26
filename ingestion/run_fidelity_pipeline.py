import os

from clean_data import clean_data
from download_fidelity_mf import data_folder, download_data, get_fund_data


def main():
    os.makedirs(data_folder, exist_ok=True)

    download_data()

    full_df = get_fund_data()
    raw_file = os.path.join(data_folder, "fidelity_funds_data.csv")
    full_df.to_csv(raw_file, index=False)

    cleaned_file = clean_data(data_folder)
    print(f"Cleaned data written to {cleaned_file}")


if __name__ == "__main__":
    main()
