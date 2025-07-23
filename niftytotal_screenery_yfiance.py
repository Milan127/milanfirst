import os
import csv
import datetime
import yfinance as yf
import pandas as pd
import ta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime as dt
from gspread.utils import rowcol_to_a1
from datetime import timedelta


# --- GOOGLE SHEET AUTH ---
def authenticate_gsheet():
    with open('credentials.json', 'w') as f:
        f.write(os.environ['GCP_CREDS_JSON'])

    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    client = gspread.authorize(creds)
    return client

# --- YFINANCE DATA FETCH ---
def get_stock_data(symbol, start_date, end_date):
    stock_data = yf.download(symbol + ".NS", start=start_date, end=end_date)
    return stock_data

# --- CALCULATE INDICATORS ---
def get_ltp_and_dma(ticker, start_date, end_date, dma_periods):
    stock_data = get_stock_data(ticker, start_date, end_date)
    df = pd.DataFrame(index=stock_data.index)
    df['LTP'] = stock_data['Close']
    for period in dma_periods:
        df[f"{period}DMA"] = df['LTP'].rolling(window=period).mean()
    df['rsi'] = ta.momentum.RSIIndicator(df['LTP']).rsi()
    df['Ratio'] = df['LTP'] / df['124DMA']
    return df

# --- STRATEGY EVALUATION ---
def evaluate_strategy(df, stock_name):
    buy_price = None
    sell_price = None
    in_observation = False
    trades = []

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

# --- READ SYMBOLS FROM CSV ---
def read_stock_symbols_from_csv(file_path):
    df = pd.read_csv(file_path)
    return df['Symbol'].tolist()

# --- STEP 6: PUSH TO GOOGLE SHEETS ---
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

# --- MAIN DRIVER ---
def main():
    client = authenticate_gsheet()
    start_date = "2024-10-01"
    end_date = (dt.today() + timedelta(days=1)).strftime('%Y-%m-%d')

    dma_periods = [20, 50, 124, 200]
    stocks = read_stock_symbols_from_csv('ind_niftyA00list.csv')

    all_trades = []

    for stock in stocks:
        df = get_ltp_and_dma(stock, start_date, end_date, dma_periods)
        trades = evaluate_strategy(df, stock)
        if trades:
            all_trades.extend(trades)

    if all_trades:
        final_df = pd.DataFrame(all_trades)
        final_df['Date'] = pd.to_datetime(final_df['Date']).dt.strftime('%d-%m-%Y')
        final_df.sort_values(by=['Stock', 'Date'], inplace=True)
        update_sheet('SRTbk1total', final_df, 'Sheet1', client)
    else:
        print("⚠️ No trades generated.")

if __name__ == "__main__":
    main()
