import pandas as pd
import urllib.parse
import pytz
import requests
import os
import ta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
from gspread.utils import rowcol_to_a1

# Timezone
TIME_ZONE = pytz.timezone('Asia/Kolkata')

# --- STEP 1: AUTHENTICATE WITH GOOGLE SHEETS ---
def authenticate_gsheet():
    # Write credentials JSON from GitHub Secret to file
    with open('credentials.json', 'w') as f:
        f.write(os.environ['GCP_CREDS_JSON'])

    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    client = gspread.authorize(creds)
    return client


# --- STEP 2: FETCH INSTRUMENTS + NIFTY 200 LIST ---
def load_symbols():
    symbol_url = 'https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz'
    symboldf = pd.read_csv(symbol_url)
    symboldf['expiry'] = pd.to_datetime(symboldf['expiry'], errors='coerce').dt.date
    symboldf = symboldf[symboldf.exchange == 'NSE_EQ']

    nifty_200 = pd.read_csv('ind_nifty200list.csv')  # <-- Upload this CSV to GitHub
    isinList_200 = 'NSE_EQ|' + nifty_200['ISIN Code'].astype(str).str.strip()
    nifty_200_df = symboldf[symboldf.instrument_key.isin(isinList_200)]
    return nifty_200_df


# --- STEP 3: FETCH HISTORICAL CANDLE DATA ---
def fetch_historical_candle_data(instrument_key):
    try:
        encoded_key = urllib.parse.quote(instrument_key)
        from_date = "2024-10-01"
        to_date = (datetime.now(TIME_ZONE) + timedelta(days=1)).strftime("%Y-%m-%d")

        url = f'https://api.upstox.com/v2/historical-candle/{encoded_key}/day/{to_date}/{from_date}'
        headers = {'accept': 'application/json'}
        res = requests.get(url, headers=headers, timeout=5.0)
        candle_data = res.json()

        if 'data' in candle_data and 'candles' in candle_data['data'] and candle_data['data']['candles']:
            df = pd.DataFrame(candle_data['data']['candles'])
            df.columns = ['date', 'Open', 'High', 'Low', 'Close', 'Volume', 'OI']
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
            df.sort_index(inplace=True)
            return df
        else:
            return None
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None


# --- STEP 4: CALCULATE INDICATORS ---
def prepare_indicators(df):
    df = df.copy()
    df['LTP'] = df['Close']
    df['124DMA'] = df['LTP'].rolling(window=124).mean()
    df['Ratio'] = df['LTP'] / df['124DMA']
    df['rsi'] = ta.momentum.RSIIndicator(close=df['LTP'], window=14).rsi()
    return df


# --- STEP 5: STRATEGY LOGIC ---
def evaluate_strategy(df, stock_name):
    trades = []
    buy_price = None
    sell_price = None
    in_observation = False

    for i in range(len(df)):
        date = df.index[i]
        row = df.iloc[i]
        rsi = row['rsi']
        ratio = row['Ratio']
        ltp = row['LTP']

        if not in_observation and rsi < 30 and ratio < 0.80:
            in_observation = True

        elif in_observation and rsi > 30 and buy_price is None:
            buy_price = ltp
            trades.append({"Stock": stock_name, "Date": date, "Action": "Buy", "Price": ltp, "RSI": rsi, "Ratio": ratio})
            in_observation = False

        if buy_price is not None and (ratio > 1.30 or rsi > 70 or ltp < 0.75 * buy_price):
            sell_price = ltp
            trades.append({"Stock": stock_name, "Date": date, "Action": "Sell", "Price": ltp, "RSI": rsi, "Ratio": ratio})
            trades.append({"Stock": stock_name, "Date": date, "Action": "Profit/Loss", "Price": sell_price - buy_price, "RSI": rsi, "Ratio": ratio})
            buy_price = None
            sell_price = None
            in_observation = False

    return trades


# --- STEP 6: PUSH TO GOOGLE SHEET ---
def update_sheet(file_name, df, sheet_name, client):
    try:
        spreadsheet = client.open(file_name)

        try:
            worksheet = spreadsheet.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            print(f"❌ Worksheet '{sheet_name}' not found.")
            return

        header_row = 2
        data_start_row = 3
        num_rows = len(df)
        num_cols = len(df.columns)

        last_cell = rowcol_to_a1(data_start_row + num_rows - 1, num_cols)
        clear_range = f"A{data_start_row}:{last_cell[:-1]}{data_start_row + num_rows - 1}"

        worksheet.batch_clear([clear_range])
        worksheet.update(f'A{header_row}', [df.columns.tolist()])
        worksheet.update(f'A{data_start_row}', df.values.tolist())

        print(f"✅ Google Sheet '{file_name}' updated successfully.")

    except Exception as e:
        print(f"❌ Sheet update failed: {e}")


# --- MAIN DRIVER CODE ---
def main():
    client = authenticate_gsheet()
    nifty_df = load_symbols()

    all_trades = []

    for _, row in nifty_df.iterrows():
        stock = row['tradingsymbol']
        inst_key = row['instrument_key']
        print(f"🔍 Processing: {stock}")

        df = fetch_historical_candle_data(inst_key)
        if df is None or df.empty:
            continue

        df = prepare_indicators(df)
        trades = evaluate_strategy(df, stock)

        if trades:
            all_trades.extend(trades)

    if all_trades:
        final_df = pd.DataFrame(all_trades)
        final_df['Date'] = pd.to_datetime(final_df['Date']).dt.tz_localize(None)
        final_df.sort_values(by=['Stock', 'Date'], inplace=True)
        final_df['Date'] = final_df['Date'].dt.strftime('%d-%m-%Y')
        

        update_sheet('SRTbk1', final_df, 'Sheet1', client)
    else:
        print("⚠️ No trades generated.")

if __name__ == "__main__":
    main()
