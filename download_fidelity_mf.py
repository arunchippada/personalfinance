import os
import requests
import pandas as pd

from datetime import datetime

data_folder = os.path.join("data", datetime.now().strftime('%Y-%m-%d'))
fund_count = 12976
page_size = 500

def get_page_count():
	return (fund_count + page_size - 1) // page_size

def download_page(url, headers, page_number):
	payload = {
		"businessChannel": "RETAIL",
		"currentPageNumber": page_number,
		"noOfRowsPerPage": page_size,
		"searchFilter": {
			"includeLeveragedAndInverseFunds": "N",
			"openToNewInvestors": "OPEN",
			"investmentTypeCode": "MFN,ETF"
		},
		"sortBy": "grossXpnsRatio",
		"sortOrder": "desc",
		"subjectAreaCode": "fundInformation,fundPicks,dailyPerformance,bestAvailableMonthlyPerformance,expensesAndFees,mstarRatings,mstarCatgAvgRiskRatings,volatility,portfolioManager,totalNetAssets,holdingCharacteristics,fundFeatures,mstarRankings,fixedIncomeCharacteristics,monthlyYields,distributions,dailyNAV",
		"tabNames": "Overview,Risk,ManagementAndFees,MorningstarRankings,IncomeCharacteristics,ShortTermPerformance,DailyPricingAndYields"
	}

	response = requests.post(url, headers=headers, json=payload)
	if response.status_code != 200:
		print(f"Failed to download page {page_number}: {response.status_code}")
		return

	filename = f"{data_folder}/funds_page_{page_number}.xlsx"
	with open(filename, "wb") as f:
		f.write(response.content)

def download_data():
	url = "https://fundresearch.fidelity.com/fund-screener/api/search/v1/funds/xlsx"
	headers = {
		"User-Agent": "Mozilla/5.0",
		"Content-Type": "application/json",
	}

	page_count = get_page_count()
	for page in range(1, page_count + 1):
		print(f"Downloading page {page}...")
		download_page(url, headers, page)

def excel_to_dataframe(filename, nrows):
	xls = pd.ExcelFile(filename, engine="openpyxl")
	dfs = {sheet_name: xls.parse(sheet_name) for sheet_name in xls.sheet_names}
	tabs = [v[:nrows] for v in dfs.values()]
	full_df = pd.concat(tabs, axis=1)
	full_df = full_df.loc[:, ~full_df.columns.duplicated()]
	return full_df

def get_fund_data():
	page_count = get_page_count()
	all_data = []
	for page in range(1, page_count + 1):
		filename = f"{data_folder}/funds_page_{page}.xlsx"
		if os.path.exists(filename):
			nrows = page_size if page < page_count else fund_count % page_size or page_size
			print(f"Adding {nrows} from {filename}... to DataFrame")
			df = excel_to_dataframe(filename, nrows)
			all_data.append(df)
		else:
			print(f"File {filename} does not exist. Skipping.")
	
	return pd.concat(all_data, ignore_index=True)

if __name__ == "__main__":
	os.makedirs(data_folder, exist_ok=True)
	download_data()
	full_df = get_fund_data()
	full_df.to_csv(f"{data_folder}/fidelity_funds_data.csv", index=False)
