import os
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pytz
import ta

# Authenticate Google Sheets
def authenticate_gsheet():
    with open('credentials.json', 'w') as f:
        f.write(os.environ['GCP_CREDS_JSON'])

    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    client = gspread.authorize(creds)
    return client

# Process stock data
def process_stocks(stocks):
    end_date = datetime.today()
    start_date = end_date - timedelta(days=5 * 365)
    hist = yf.download(stocks, start=start_date, end=end_date + timedelta(days=1), group_by='ticker', auto_adjust=False)

    final_data = []

    for stock in stocks:
        try:
            df = hist[stock].copy()
            df.reset_index(inplace=True)
            df.sort_values('Date', inplace=True)

            df['20D_Low'] = df['Low'].rolling(window=20).min()
            df['20D_High'] = df['High'].rolling(window=20).max()
            df['Prev_20D_High'] = df['20D_High'].shift(1)

            # RSI Daily
            df['RSI_D'] = ta.momentum.RSIIndicator(df['Close'], window=14).rsi()
            rsi_d = df['RSI_D'].iloc[-1]

            # ADX Daily
            adx_indicator = ta.trend.ADXIndicator(high=df['High'], low=df['Low'], close=df['Close'], window=14)
            df['ADX_D'] = adx_indicator.adx()
            adx_d = df['ADX_D'].iloc[-1]

            today = df['Date'].iloc[-1]
            today_close = df['Close'].iloc[-1]
            prev_close = df['Close'].iloc[-2] if len(df) >= 2 else None
            today_change = ((today_close - prev_close) / prev_close) * 100 if prev_close else None

            old_gtt = df['Prev_20D_High'].iloc[-1]
            new_gtt = df['20D_High'].iloc[-1]

            mask_20d_low = df['Low'] == df['20D_Low']
            latest_20d_low_date = df.loc[mask_20d_low, 'Date'].max()
            latest_20d_low_price = df.loc[df['Date'] == latest_20d_low_date, 'Low'].values[0] if pd.notnull(latest_20d_low_date) else None

            df_1y = df[df['Date'] >= (end_date - timedelta(days=365))]
            high_52w = df_1y['High'].max()
            high_52w_date = df_1y[df_1y['High'] == high_52w]['Date'].values[0]
            low_52w = df_1y['Low'].min()
            low_52w_date = df_1y[df_1y['Low'] == low_52w]['Date'].values[0]

            boh_eligible = "YES" if low_52w_date > high_52w_date else ""

            trigger_date = None
            gtt_trigger_price = None
            if pd.notnull(latest_20d_low_date):
                df_after_low = df[df['Date'] > latest_20d_low_date].copy()
                df_after_low = df_after_low[df_after_low['High'] >= df_after_low['Prev_20D_High']]
                if not df_after_low.empty:
                    trigger_date = df_after_low.iloc[0]['Date']
                    gtt_trigger_price = df_after_low.iloc[0]['Prev_20D_High']

            pnl_percent = None
            if trigger_date and gtt_trigger_price:
                pnl_percent = ((today_close - gtt_trigger_price) / gtt_trigger_price) * 100

            percent_diff = None
            if not trigger_date and new_gtt and today_close:
                percent_diff = ((new_gtt - today_close) / today_close) * 100

            today_str = end_date.strftime('%d-%b-%Y')
            latest_20d_low_date_str = latest_20d_low_date.strftime('%d-%b-%Y') if pd.notnull(latest_20d_low_date) else None
            trigger_date_str = trigger_date.strftime('%d-%b-%Y') if pd.notnull(trigger_date) else None

            if trigger_date_str:
                gtt_update = "TRIGGERED"
            elif old_gtt != new_gtt:
                gtt_update = "YES"
            elif latest_20d_low_date_str == today_str:
                gtt_update = "NEW ADD"
            else:
                gtt_update = ""

            final_data.append({
                'Ticker': stock.replace('.NS', ''),
                '20D LOW DATE': latest_20d_low_date_str,
                '20D LOW': f"{latest_20d_low_price:.2f}" if latest_20d_low_price else None,
                'OLD GTT': f"{old_gtt:.2f}" if pd.notnull(old_gtt) else None,
                'NEW GTT': f"{new_gtt:.2f}" if pd.notnull(new_gtt) else None,
                'CLOSE': f"{today_close:.2f}" if pd.notnull(today_close) else None,
                'TODAY CHANGE': f"{today_change:.2f}" if today_change is not None else None,
                '% DIFF': f"{percent_diff:.2f}" if percent_diff is not None else None,
                'GTT UPDATE': gtt_update,
                'BOH ELIGIBLE': boh_eligible,
                'TRIGGER DATE': trigger_date_str,
                'GTT TRIGGER PRICE': f"{gtt_trigger_price:.2f}" if gtt_trigger_price else None,
                'P&L %': f"{pnl_percent:.2f}" if pnl_percent is not None else None,
                'RSI D': f"{rsi_d:.2f}" if pd.notnull(rsi_d) else None,
                'ADX D': f"{adx_d:.2f}" if pd.notnull(adx_d) else None
            })

        except Exception as e:
            print(f"Error processing {stock}: {e}")

    return pd.DataFrame(final_data)

# Read symbol lists
nifty50 = pd.read_csv("ind_nifty50list.csv")['Symbol'].str.upper().tolist()
nifty100 = pd.read_csv("ind_niftynext50list.csv")['Symbol'].str.upper().tolist()
nifty200 = pd.read_csv("ind_nifty200list.csv")['Symbol'].str.upper().tolist()

nifty100 = [s for s in nifty100 if s not in nifty50]
nifty200 = [s for s in nifty200 if s not in nifty50 and s not in nifty100]

nifty50 = [s + ".NS" for s in nifty50]
nifty100 = [s + ".NS" for s in nifty100]
nifty200 = [s + ".NS" for s in nifty200]

client = authenticate_gsheet()

# Update Google Sheet
def update_sheet(file_name, df, sheet_name):
    try:
        spreadsheet = client.open(file_name)
        worksheet = spreadsheet.worksheet(sheet_name)

        start_row = 4
        end_row = len(df) + start_row - 1
        clear_range = f'A{start_row}:Z{end_row}'
        worksheet.batch_clear([clear_range])

        worksheet.update(range_name=f'A{start_row}', values=df.values.tolist())

        try:
            ist = pytz.timezone('Asia/Kolkata')
            update_timestamp = datetime.now(ist).strftime("Last Update: %d-%m-%Y %I:%M:%S %p")
            worksheet.update('A1', [[update_timestamp]])
            print(f"Updated cell A1 with timestamp '{update_timestamp}'.")
        except gspread.exceptions.APIError as e:
            print(f"API Error updating cell A1 in '{sheet_name}': {e}")

    except Exception as e:
        print(f"Failed to update '{sheet_name}': {e}")

# Run process
df_nifty50 = process_stocks(nifty50)
df_nifty100 = process_stocks(nifty100)
df_nifty200 = process_stocks(nifty200)

update_sheet('SST WITH RSI AND RS  BY MILAN YFINACE', df_nifty50, 'SST-N50')
update_sheet('SST WITH RSI AND RS  BY MILAN YFINACE', df_nifty100, 'SST-N100')
update_sheet('SST WITH RSI AND RS  BY MILAN YFINACE', df_nifty200, 'SST-N200')
