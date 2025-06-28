import pandas as pd
import urllib.parse
import pytz
import requests
from datetime import datetime, timedelta
import ta
import gspread
import json
import os
from oauth2client.service_account import ServiceAccountCredentials

TIME_ZONE = pytz.timezone('Asia/Kolkata')

# Write credentials.json file from env var (for GitHub Actions)
if os.getenv("GOOGLE_CREDENTIALS_JSON"):
    with open("credentials.json", "w") as f:
        f.write(os.getenv("GCP_CREDS_JSON"))

# Load symbols
fileUrl = 'https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz'
symboldf = pd.read_csv(fileUrl)
symboldf['expiry'] = pd.to_datetime(symboldf['expiry'], errors='coerce').dt.date
symboldf = symboldf[symboldf.exchange == 'NSE_EQ']

# Load ISIN lists
nifty_50 = pd.read_csv('ind_nifty50list.csv')
nifty_100 = pd.read_csv("ind_niftynext50list.csv")
nifty_200 = pd.read_csv("ind_nifty200list.csv")

# Create ISIN lists for filtering
isinList_50 = 'NSE_EQ|' + nifty_50['ISIN Code'].astype(str).str.strip()
isinList_100 = 'NSE_EQ|' + nifty_100['ISIN Code'].astype(str).str.strip()
isinList_200 = 'NSE_EQ|' + nifty_200['ISIN Code'].astype(str).str.strip()

# Filter DataFrames
nifty_50_df = symboldf[symboldf.instrument_key.isin(isinList_50)]
nifty_100_df = symboldf[symboldf.instrument_key.isin(isinList_100)]
nifty_100_df = nifty_100_df.loc[~nifty_100_df.instrument_key.isin(nifty_50_df.instrument_key)]

nifty_200_df = symboldf[symboldf.instrument_key.isin(isinList_200)]
exclude_keys = set(nifty_50_df.instrument_key).union(set(nifty_100_df.instrument_key))
nifty_200_df = nifty_200_df.loc[~nifty_200_df.instrument_key.isin(exclude_keys)]

def getHistoricalData(symInfo):
    try:
        parseInstrument = urllib.parse.quote(symInfo.instrument_key)
        fromDate = (datetime.now(TIME_ZONE) - timedelta(days=10000)).strftime("%Y-%m-%d")
        toDate = datetime.now(TIME_ZONE).strftime("%Y-%m-%d")
        url = f'https://api.upstox.com/v2/historical-candle/{parseInstrument}/day/{toDate}/{fromDate}'

        res = requests.get(url, headers={'accept': 'application/json'}, timeout=5.0)
        candleRes = res.json()
        if 'data' not in candleRes or 'candles' not in candleRes['data']:
            return None

        hist = pd.DataFrame(candleRes['data']['candles'], columns=['date', 'Open', 'High', 'Low', 'Close', 'vol', 'oi'])
        hist['date'] = pd.to_datetime(hist['date'])
        hist.set_index('date', inplace=True)
        hist.sort_index(inplace=True)

        high_52w = hist['High'].max()
        low_52w = hist['Low'].min()
        high_52w_date = hist['High'].idxmax()
        low_52w_date = hist['Low'].idxmin()
        high_52w_date_str = high_52w_date.strftime('%d-%b-%Y')
        low_52w_date_str = low_52w_date.strftime('%d-%b-%Y')

        hist['20d Low'] = hist['Low'].rolling(window=20).min()
        hist['20d High'] = hist['High'].rolling(window=20).max()
        hist['Prev Day 20D High'] = hist['20d High'].shift(1)

        low_touch_dates = hist[hist['Low'] == hist['20d Low']].index
        if not low_touch_dates.empty:
            last_20d_low_date = low_touch_dates[-1]
            last_20d_low_price = hist.loc[last_20d_low_date, 'Low']
            last_20d_low_date_str = last_20d_low_date.strftime('%d-%b-%Y')
            last_20d_low_price_str = f"{last_20d_low_price:.2f}"
        else:
            last_20d_low_date = None
            last_20d_low_date_str = None
            last_20d_low_price_str = None

        first_high_touched_date_str = None
        first_high_touched_prev_day_20d_high_str = None
        first_high_touched_price = None

        if last_20d_low_date:
            subsequent_high_dates = hist[hist.index > last_20d_low_date]
            first_high_touched_dates = subsequent_high_dates[subsequent_high_dates['High'] >= subsequent_high_dates['Prev Day 20D High']]
            if not first_high_touched_dates.empty:
                first_high_touched_date = first_high_touched_dates.index[0]
                first_high_touched_price = hist.loc[first_high_touched_date, 'Close']
                first_high_touched_prev_day_20d_high_str = f"{hist.loc[first_high_touched_date, 'Prev Day 20D High']:.2f}"
                first_high_touched_date_str = first_high_touched_date.strftime('%d-%b-%Y')

        last_close_price = hist['Close'].iloc[-1] if not hist['Close'].empty else None
        last_close_price_str = f"{last_close_price:.2f}" if last_close_price else None

        percent_diff_str = None
        if first_high_touched_date_str is None and last_close_price and hist['20d High'].iloc[-1]:
            percent_diff = ((hist['20d High'].iloc[-1] - last_close_price) / last_close_price) * 100
            percent_diff_str = f"{percent_diff:.2f}"

        pnl_percent_str = None
        if first_high_touched_price and first_high_touched_prev_day_20d_high_str:
            ref_price = hist.loc[first_high_touched_date, 'Prev Day 20D High']
            pnl_percent = ((last_close_price - ref_price) / ref_price) * 100
            pnl_percent_str = f"{pnl_percent:.2f}"

        boh_eligibility = 'YES' if low_52w_date > high_52w_date else ''

        hist['RSI'] = ta.momentum.RSIIndicator(hist['Close']).rsi()
        hist_weekly = hist['Close'].resample('W').last().to_frame()
        hist_weekly['RSI'] = ta.momentum.RSIIndicator(hist_weekly['Close']).rsi()
        last_weekly_rsi_str = f"{hist_weekly['RSI'].iloc[-1]:.2f}" if not hist_weekly['RSI'].empty else None

        hist_monthly = hist['Close'].resample('M').last().to_frame()
        hist_monthly['RSI'] = ta.momentum.RSIIndicator(hist_monthly['Close']).rsi()
        last_monthly_rsi_str = f"{hist_monthly['RSI'].iloc[-1]:.2f}" if not hist_monthly['RSI'].empty else None

        gtt_update = ""
        if first_high_touched_date_str:
            gtt_update = "TRIGGERED"
        elif hist['Prev Day 20D High'].iloc[-1] != hist['20d High'].iloc[-1]:
            gtt_update = "YES"
        elif datetime.today().date() == last_20d_low_date.date():
            gtt_update = "NEW ADD"

        return {
            'Stock': symInfo.tradingsymbol,
            '20D LOW DATE': last_20d_low_date_str,
            '20D LOW': last_20d_low_price_str,
            'OLD GTT': f"{hist['Prev Day 20D High'].iloc[-1]:.2f}" if not hist['Prev Day 20D High'].empty else None,
            'NEW GTT': f"{hist['20d High'].iloc[-1]:.2f}" if not hist['20d High'].empty else None,
            'CLOSE': last_close_price_str,
            '%DIFF': percent_diff_str,
            'GTT Update': gtt_update,
            'BOH Eligibility': boh_eligibility,
            'TRIGGER DATE': first_high_touched_date_str,
            'GTT TRIGGER PRICE': first_high_touched_prev_day_20d_high_str,
            'P&L %': pnl_percent_str,
            'DAILY RSI': f"{hist['RSI'].iloc[-1]:.2f}" if not hist['RSI'].empty else None,
            'WEEKLY RSI': last_weekly_rsi_str,
            'MONTHLY RSI': last_monthly_rsi_str
        }

    except Exception as e:
        print(f"Error for {symInfo.tradingsymbol}: {e}")
        return None

def process_data(df):
    return pd.DataFrame([getHistoricalData(row) for _, row in df.iterrows() if getHistoricalData(row)])

def update_sheet(file_name, df, sheet_name):
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    client = gspread.authorize(creds)

    try:
        sheet = client.open(file_name).worksheet(sheet_name)
        sheet.batch_clear([f'A4:P{len(df)+3}'])
        sheet.update(f'A4', [df.columns.tolist()] + df.values.tolist())
        sheet.update('I1', [[datetime.now().strftime("Last Update: %d-%m-%Y %I:%M:%S %p")]])
        print(f"{sheet_name} updated.")
    except Exception as e:
        print(f"Sheet update error: {e}")

if __name__ == "__main__":
    update_sheet("SST WITH RSI AND RS  BY MILAN YFINACE", process_data(nifty_50_df), "SST-N50")
    update_sheet("SST WITH RSI AND RS  BY MILAN YFINACE", process_data(nifty_100_df), "SST-N100")
    update_sheet("SST WITH RSI AND RS  BY MILAN YFINACE", process_data(nifty_200_df), "SST-N200")
