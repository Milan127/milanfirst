import csv
import datetime
import yfinance as yf
import pandas as pd
import tempfile
from urllib.request import urlopen
import json
import ta
import os
import requests

# --- TELEGRAM ALERT FUNCTION ---
def send_telegram_message(message):
    token = os.environ.get('TELEGRAM_BOT_TOKEN')
    chat_id = os.environ.get('TELEGRAM_CHAT_ID')
    if not token or not chat_id:
        print("Telegram secrets not found.")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = {'chat_id': chat_id, 'text': message}
    try:
        requests.post(url, data=data, timeout=5)
    except Exception as e:
        print(f"Telegram alert failed: {e}")

# Function to get stock data from Yahoo Finance
def get_stock_data(symbol, start_date, end_date):
    stock_data = yf.download(symbol + ".NS", start=start_date, end=end_date)
    return stock_data

# Function to calculate LTP and DMA
def get_ltp_and_dma(ticker, start_date, end_date, dma_periods):
    stock_data = get_stock_data(ticker, start_date, end_date)

    dma_columns = [f"{period}DMA" for period in dma_periods]
    df = pd.DataFrame(index=stock_data.index)
    df['LTP'] = stock_data['Close']

    for period in dma_periods:
        df[f"{period}DMA"] = df['LTP'].rolling(window=period).mean()

    return df

# Function to evaluate the trading strategy
def evaluate_strategy(df, stock_name):
    buy_price = None
    sell_price = None
    in_observation = False
    observation_start_index = None
    trades = []

    for i in range(len(df)):
        date = df.index[i]
        rsi = df.iloc[i]['rsi']
        ratio = df.iloc[i]['Ratio']
        ltp = df.iloc[i]['LTP']

        if not in_observation:
            if rsi < 30 and ratio < 0.80:
                in_observation = True
                observation_start_index = i

        if in_observation:
            if rsi > 30:
                if buy_price is None:
                    buy_price = ltp
                    trades.append({
                        "Stock": stock_name,
                        "Date": date,
                        "Action": "Buy",
                        "Price": buy_price,
                        "RSI": rsi,
                        "Ratio": ratio
                    })
                    send_telegram_message(
                        f"\ud83d\udd14 BUY ALERT:\nStock: {stock_name}\nDate: {date.strftime('%d-%m-%Y')}\nPrice: \u20b9{ltp:.2f}\nRSI: {rsi:.2f} | Ratio: {ratio:.2f}"
                    )
                    in_observation = False
                    observation_start_index = None

        if ratio > 1.30 or rsi > 70 or (buy_price is not None and ltp < 0.75 * buy_price):
            if buy_price is not None and sell_price is None:
                sell_price = ltp
                trades.append({
                    "Stock": stock_name,
                    "Date": date,
                    "Action": "Sell",
                    "Price": sell_price,
                    "RSI": rsi,
                    "Ratio": ratio
                })
                trades.append({
                    "Stock": stock_name,
                    "Date": date,
                    "Action": "Profit/Loss",
                    "Price": (sell_price - buy_price),
                    "RSI": rsi,
                    "Ratio": ratio
                })
                send_telegram_message(
                    f"\u2757 SELL ALERT:\nStock: {stock_name}\nDate: {date.strftime('%d-%m-%Y')}\nPrice: \u20b9{ltp:.2f}\nRSI: {rsi:.2f} | Ratio: {ratio:.2f}"
                )
                buy_price = None
                sell_price = None
                in_observation = False
                observation_start_index = None

    return trades

# Function to read stock symbols from CSV
def read_stock_symbols_from_csv(file_path):
    df = pd.read_csv(file_path)
    return df['Symbol'].tolist()  # Assuming 'Symbol' is the column name for stock symbols

# --- MAIN DRIVER ---
def main():
    start_date = "2022-10-01"
    end_date = datetime.datetime.today().strftime('%Y-%m-%d')
    dma_periods = [200, 50, 20, 124]
    csv_file_path = 'ind_nifty200list.csv'

    stocks = read_stock_symbols_from_csv(csv_file_path)
    all_data = []
    all_trades = []

    for stock in stocks:
        try:
            ltp_and_dma_table = get_ltp_and_dma(stock, start_date, end_date, dma_periods)
            ltp_and_dma_table['rsi'] = ta.momentum.RSIIndicator(ltp_and_dma_table['LTP']).rsi()
            ltp_and_dma_table['Ratio'] = ltp_and_dma_table['LTP'] / ltp_and_dma_table['124DMA']
            trades = evaluate_strategy(ltp_and_dma_table, stock)
            all_data.append((stock, trades))
            all_trades.extend(trades)
        except Exception as e:
            send_telegram_message(f"\u26a0\ufe0f ERROR in {stock}: {str(e)}")

    if all_trades:
        combined_trades = pd.DataFrame(all_trades)
        combined_trades.to_excel("combined_trades.xlsx", index=False)
        send_telegram_message(f"\ud83d\udcca Daily Summary: {datetime.datetime.today().strftime('%d-%m-%Y')}\nTotal Trades: {len(all_trades)}")
    else:
        send_telegram_message("\u26a0\ufe0f No trades generated today.")

if __name__ == "__main__":
    main()
