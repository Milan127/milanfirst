import pandas as pd
import urllib.parse
import pytz
import requests
import ta
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from gspread.utils import rowcol_to_a1
from tqdm import tqdm

TIME_ZONE = pytz.timezone('Asia/Kolkata')

# ==============================
# AUTH GOOGLE SHEETS
# ==============================
def authenticate_gsheet():
    with open('credentials.json', 'w') as f:
        f.write(os.environ['GCP_CREDS_JSON'])

    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']

    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    return gspread.authorize(creds)


# ==============================
# LOAD SYMBOLS (ETF CSV)
# ==============================
def load_symbols():

    symbol_url = 'https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz'
    symboldf = pd.read_csv(symbol_url)
    symboldf = symboldf[symboldf.exchange == 'NSE_EQ']

    mw_df = pd.read_csv('MW-ETF-21-Feb-2026.csv')
    mw_df.columns = mw_df.columns.str.strip()

    symbol_list = mw_df['SYMBOL'].str.strip().tolist()

    symboldf['tradingsymbol'] = symboldf['tradingsymbol'].str.replace('-EQ', '', regex=False)
    filtered_df = symboldf[symboldf['tradingsymbol'].isin(symbol_list)]

    print(f"✅ Symbols matched: {len(filtered_df)}")
    return filtered_df


# ==============================
# FETCH DATA
# ==============================
def fetch_historical_candle_data(instrument_key):

    try:
        encoded_key = urllib.parse.quote(instrument_key)

        from_date = "2024-10-01"
        to_date = (datetime.now(TIME_ZONE) + timedelta(days=1)).strftime("%Y-%m-%d")

        url = f'https://api.upstox.com/v2/historical-candle/{encoded_key}/day/{to_date}/{from_date}'

        res = requests.get(url, headers={'accept': 'application/json'}, timeout=5.0)
        data = res.json()

        if 'data' not in data:
            return None

        df = pd.DataFrame(data['data']['candles'])
        df.columns = ['date', 'Open', 'High', 'Low', 'Close', 'Volume', 'OI']
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        df.sort_index(inplace=True)

        return df

    except:
        return None


# ==============================
# INDICATORS
# ==============================
def prepare_indicators(df):

    df['LTP'] = df['Close']
    df['124DMA'] = df['LTP'].rolling(124).mean()
    df['Ratio'] = df['LTP'] / df['124DMA']
    df['RSI'] = ta.momentum.RSIIndicator(df['LTP']).rsi()

    return df


# ==============================
# STRATEGY
# ==============================
def evaluate_strategy(df, stock_name, underlying):

    trades = []
    buy_price = None
    in_observation = False

    for i in range(len(df)):

        date = df.index[i]
        row = df.iloc[i]

        rsi = row['RSI']
        ratio = row['Ratio']
        ltp = row['LTP']

        if not in_observation and rsi < 30 and ratio < 0.90:
            in_observation = True

        elif in_observation and rsi > 30 and buy_price is None:
            buy_price = ltp
            trades.append({
                "Stock": stock_name,
                "Date": date,
                "Action": "Buy",
                "Price": ltp,
                "RSI": rsi,
                "Ratio": ratio,
                "Underlying": underlying
            })
            in_observation = False

        if buy_price and (ratio > 1.30 or rsi > 75 or ltp < 0.75 * buy_price):

            trades.append({
                "Stock": stock_name,
                "Date": date,
                "Action": "Sell",
                "Price": ltp,
                "RSI": rsi,
                "Ratio": ratio,
                "Underlying": underlying
            })

            trades.append({
                "Stock": stock_name,
                "Date": date,
                "Action": "Profit/Loss",
                "Price": ltp - buy_price,
                "RSI": rsi,
                "Ratio": ratio,
                "Underlying": underlying
            })

            buy_price = None
            in_observation = False

    return trades


# ==============================
# PROCESS SINGLE STOCK
# ==============================
def process_stock(row):

    stock = row['tradingsymbol']
    inst_key = row['instrument_key']
    underlying = row['name']

    df = fetch_historical_candle_data(inst_key)

    if df is None or df.empty:
        return []

    df = prepare_indicators(df)

    return evaluate_strategy(df, stock, underlying)


# ==============================
# UPDATE GOOGLE SHEET
# ==============================
def update_sheet(df, client):

    spreadsheet = client.open('SRTbk1')
    worksheet = spreadsheet.worksheet('Sheet1')

    header_row = 2
    data_start_row = 3

    num_rows = len(df)
    num_cols = len(df.columns)

    last_cell = rowcol_to_a1(data_start_row + num_rows - 1, num_cols)

    worksheet.batch_clear([f"A{data_start_row}:{last_cell}"])

    worksheet.update(f'A{header_row}', [df.columns.tolist()])
    worksheet.update(f'A{data_start_row}', df.values.tolist())

    print("✅ Google Sheet Updated")


# ==============================
# MAIN
# ==============================
def main():

    client = authenticate_gsheet()
    symbols_df = load_symbols()

    all_trades = []

    print("🚀 Fetching data with multi-threading...\n")

    with ThreadPoolExecutor(max_workers=10) as executor:

        futures = [executor.submit(process_stock, row) for _, row in symbols_df.iterrows()]

        for future in tqdm(as_completed(futures), total=len(futures)):
            result = future.result()
            if result:
                all_trades.extend(result)

    if all_trades:

        final_df = pd.DataFrame(all_trades)

        final_df['Date'] = pd.to_datetime(final_df['Date']).dt.strftime('%d-%m-%Y')

        final_df = final_df[
            ["Stock","Date","Action","Price","RSI","Ratio","Underlying"]
        ]

        update_sheet(final_df, client)

    else:
        print("⚠️ No trades generated")


if __name__ == "__main__":
    main()
