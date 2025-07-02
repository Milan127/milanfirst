import os
import csv
import ta
import pytz
import yfinance as yf
import datetime
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread.utils import rowcol_to_a1

TIME_ZONE = pytz.timezone('Asia/Kolkata')

# --- STEP 1: GOOGLE SHEETS AUTH ---
def authenticate_gsheet():
    with open('credentials.json', 'w') as f:
        f.write(os.environ['GCP_CREDS_JSON'])
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    return gspread.authorize(creds)

# --- STEP 2: READ SYMBOLS FROM CSV ---
def read_stock_symbols_from_csv(file_path):
    df = pd.read_csv(file_path)
    return df['Symbol'].tolist()

# --- STEP 3: FETCH HISTORICAL DATA ---
def get_stock_data(symbol, start_date, end_date):
    return yf.download(symbol + ".NS", start=start_date, end=end_date)

def get_ltp_and_dma(ticker, start_date, end_date, dma_periods):
    stock_data = get_stock_data(ticker, start_date, end_date)
    if stock_data.empty:
        return None
    df = pd.DataFrame(index=stock_data.index)
    df['LTP'] = stock_data['Close']
    for period in dma_periods:
        df[f"{period}DMA"] = df['LTP'].rolling(window=period).mean()
    return df

# --- STEP 4: STRATEGY LOGIC ---
def evaluate_strategy(df, stock_name):
    buy_price = None
    sell_price = None
    in_observation = False
    trades = []

    for i in range(len(df)):
        date = df.index[i]
        rsi = df.iloc[i]['rsi']
        ratio = df.iloc[i]['Ratio']
        ltp = df.iloc[i]['LTP']

        if not in_observation and rsi < 30 and ratio < 0.80:
            in_observation = True

        if in_observation and rsi > 30:
            if buy_price is None:
                buy_price = ltp
                trades.append({"Stock": stock_name, "Date": date, "Action": "Buy", "Price": ltp, "RSI": rsi, "Ratio": ratio})
                in_observation = False

        if buy_price is not None and (ratio > 1.30 or rsi > 70 or ltp < 0.75 * buy_price):
            sell_price = ltp
            trades.append({"Stock": stock_name, "Date": date, "Action": "Sell", "Price": ltp, "RSI": rsi, "Ratio": ratio})
            trades.append({"Stock": stock_name, "Date": date, "Action": "Profit/Loss", "Price": ltp - buy_price, "RSI": rsi, "Ratio": ratio})
            buy_price = None
            sell_price = None
            in_observation = False

    return trades

# --- STEP 5: PUSH TO GOOGLE SHEET ---
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

# --- MAIN FUNCTION ---
def main():
    start_date = "2022-10-01"
    end_date = datetime.datetime.today().strftime('%Y-%m-%d')
    dma_periods = [200, 50, 20, 124]
    csv_file_path = 'ind_nifty200list.csv'  # should be in the repo
    stocks = read_stock_symbols_from_csv(csv_file_path)

    all_trades = []
    for stock in stocks:
        print(f"🔍 Processing: {stock}")
        df = get_ltp_and_dma(stock, start_date, end_date, dma_periods)
        if df is None or df.empty:
            continue
        df['rsi'] = ta.momentum.RSIIndicator(df['LTP']).rsi()
        df['Ratio'] = df['LTP'] / df['124DMA']
        trades = evaluate_strategy(df, stock)
        if trades:
            all_trades.extend(trades)

    if all_trades:
        final_df = pd.DataFrame(all_trades)
        final_df['Date'] = pd.to_datetime(final_df['Date']).dt.tz_localize(None)
        final_df.sort_values(by=['Stock', 'Date'], inplace=True)
        final_df['Date'] = final_df['Date'].dt.strftime('%d-%m-%Y')

        client = authenticate_gsheet()
        update_sheet('SRTbk1 yf', final_df, 'Sheet1', client)
    else:
        print("⚠️ No trades generated.")

if __name__ == "__main__":
    main()
