import os
import pandas as pd
import urllib.parse
import pytz
import requests
from datetime import datetime, timedelta
import ta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import timedelta


TIME_ZONE = pytz.timezone('Asia/Kolkata')

# --- STEP 1: AUTHENTICATE WITH GOOGLE SHEETS ---
def authenticate_gsheet():
    # Write credentials JSON from GitHub Secret to file
    with open('credentials.json', 'w') as f:
        f.write(os.environ['GCP_CREDS_JSON'])


# Load symbols
fileUrl = 'https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz'
symboldf = pd.read_csv(fileUrl)
symboldf['expiry'] = pd.to_datetime(symboldf['expiry']).apply(lambda x: x.date())
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

# Remove duplicates from nifty_100_df
nifty_100_df = nifty_100_df.loc[~nifty_100_df.instrument_key.isin(nifty_50_df.instrument_key)]

# Now process nifty_200_df
nifty_200_df = symboldf[symboldf.instrument_key.isin(isinList_200)]

# Use set difference to remove duplicates from nifty_200_df
exclude_keys = set(nifty_50_df.instrument_key).union(set(nifty_100_df.instrument_key))
nifty_200_df = nifty_200_df.loc[~nifty_200_df.instrument_key.isin(exclude_keys)]






def getHistoricalData(symInfo):
    try:
        parseInstrument = urllib.parse.quote(symInfo.instrument_key)
        fromDate = (datetime.now(TIME_ZONE) - timedelta(days=1000)).strftime("%Y-%m-%d")
        to_date = (datetime.now(TIME_ZONE) + timedelta(days=1)).strftime("%Y-%m-%d")

        url = f'https://api.upstox.com/v2/historical-candle/{parseInstrument}/day/{toDate}/{fromDate}'
        
        res = requests.get(url, headers={'accept': 'application/json'}, params={}, timeout=5.0)
        candleRes = res.json()
        
        if 'data' in candleRes and 'candles' in candleRes['data'] and candleRes['data']['candles']:
            hist = pd.DataFrame(candleRes['data']['candles'])
            hist.columns = ['date', 'Open', 'High', 'Low', 'Close', 'vol', 'oi']
            hist['date'] = pd.to_datetime(hist['date'])
            hist.set_index('date', inplace=True)
            
        hist.sort_values(by="date", ascending=True,inplace=True)    
        # Calculate 52-week high and low
        high_52w = hist['High'].max()
        low_52w = hist['Low'].min()
        high_52w_date = hist['High'].idxmax()
        low_52w_date = hist['Low'].idxmin()

        # Convert dates to strings for output
        high_52w_date_str = high_52w_date.strftime('%d-%b-%Y')
        low_52w_date_str = low_52w_date.strftime('%d-%b-%Y')

        # Calculate 20-day low and high
        hist['20d Low'] = hist['Low'].rolling(window=20).min()
        hist['20d High'] = hist['High'].rolling(window=20).max()
        
        # Calculate previous day's 20-day high for each day
        hist['Prev Day 20D High'] = hist['20d High'].shift(1)
        
        # Find the date and price of the 20-day low
        low_touch_dates = hist[hist['Low'] == hist['20d Low']].index
        if not low_touch_dates.empty:
            last_20d_low_date = low_touch_dates[-1]
            last_20d_low_price = hist.loc[last_20d_low_date, 'Low']
            last_20d_low_date_str = last_20d_low_date.strftime('%d-%b-%Y')
            last_20d_low_price_str = f"{last_20d_low_price:.2f}"
        else:
            last_20d_low_date_str = None
            last_20d_low_price_str = None

        # Find the date when the previous day's 20-day high was first touched after the 20-day low date
        first_high_touched_price = None
        if last_20d_low_date:
            subsequent_high_dates = hist[hist.index > last_20d_low_date]
            first_high_touched_dates = subsequent_high_dates[subsequent_high_dates['High'] >= subsequent_high_dates['Prev Day 20D High']]
            
            if not first_high_touched_dates.empty:
                first_high_touched_date = first_high_touched_dates.index[0]
                first_high_touched_date_str = first_high_touched_date.strftime('%d-%b-%Y')
                
                # Get the closing price on the day when the first high was touched
                first_high_touched_price = hist.loc[first_high_touched_date, 'Close']
                first_high_touched_prev_day_20d_high_str = f"{hist.loc[first_high_touched_date, 'Prev Day 20D High']:.2f}"
            else:
                first_high_touched_date_str = None
                first_high_touched_prev_day_20d_high_str = None
        else:
            first_high_touched_date_str = None
            first_high_touched_prev_day_20d_high_str = None

        # Last close price
        last_close_price = hist['Close'].iloc[-1] if not hist['Close'].empty else None
        last_close_price_str = f"{last_close_price:.2f}" if last_close_price is not None else None

        # Calculate %DIFF
        percent_diff_str = None
        if first_high_touched_date_str is None and last_close_price and hist['20d High'].iloc[-1]:
            percent_diff = ((hist['20d High'].iloc[-1] - last_close_price) / last_close_price) * 100
            percent_diff_str = f"{percent_diff:.2f}"
        
        # Calculate P&L %
        pnl_percent_str = None
        if first_high_touched_prev_day_20d_high_str is not None and first_high_touched_price is not None:
            pnl_percent = ((last_close_price - hist.loc[first_high_touched_date, 'Prev Day 20D High']) / hist.loc[first_high_touched_date, 'Prev Day 20D High']) * 100
            pnl_percent_str = f"{pnl_percent:.2f}"
        else:
            pnl_percent_str = None

        # BOH Eligibility: 'YES' if 52W Low Date is after 52W High Date
        boh_eligibility = 'YES' if low_52w_date > high_52w_date else ''

        # Calculate daily RSI
        hist['RSI'] = ta.momentum.RSIIndicator(hist['Close']).rsi()
        
        # Calculate weekly RSI
        hist_weekly = hist.resample('W').agg({'Close': 'last'})
        hist_weekly['RSI'] = ta.momentum.RSIIndicator(hist_weekly['Close']).rsi()
        last_weekly_rsi = hist_weekly['RSI'].iloc[-1] if not hist_weekly['RSI'].empty else None
        last_weekly_rsi_str = f"{last_weekly_rsi:.2f}" if last_weekly_rsi is not None else None

        # Calculate monthly RSI
        hist_monthly = hist.resample('M').agg({'Close': 'last'})
        hist_monthly['RSI'] = ta.momentum.RSIIndicator(hist_monthly['Close']).rsi()
        last_monthly_rsi = hist_monthly['RSI'].iloc[-1] if not hist_monthly['RSI'].empty else None
        last_monthly_rsi_str = f"{last_monthly_rsi:.2f}" if last_monthly_rsi is not None else None

        #  GTT Update: Equivalent to ARRAYFORMULA logic
        gtt_update = ""
        if first_high_touched_date_str is not None:
            gtt_update = "TRIGGERED"
         
        elif hist['Prev Day 20D High'].iloc[-1] != hist['20d High'].iloc[-1]:
            gtt_update = "YES"
        elif datetime.today().date() == last_20d_low_date.date():
            gtt_update = "NEW ADD"
            
        return {
            'Stock': symInfo.tradingsymbol	,
            '20D LOW DATE': last_20d_low_date_str,
            '20D LOW': last_20d_low_price_str,
            'OLD GTT': f"{hist['Prev Day 20D High'].iloc[-1]:.2f}" if not hist['Prev Day 20D High'].empty else None,
            'NEW GTT': f"{hist['20d High'].iloc[-1]:.2f}" if not hist['20d High'].empty else None,
            'CLOSE': last_close_price_str,  # Add the last close price column
            '%DIFF': percent_diff_str,  # Add %DIFF column
            'GTT Update': gtt_update,  # Add a blank column for GTT updates
            'BOH Eligibility': boh_eligibility,  # Add BOH Eligibility column
            'TRIGGER DATE': first_high_touched_date_str,
            'GTT TRIGGER PRICE': first_high_touched_prev_day_20d_high_str,
            'P&L %': pnl_percent_str,  # Add P&L % column
            'DAILY RSI': f"{hist['RSI'].iloc[-1]:.2f}" if not hist['RSI'].empty else None,  # Add Daily RSI column
            'WEEKLY RSI': last_weekly_rsi_str,  # Add Weekly RSI column
            'MONTHLY RSI': last_monthly_rsi_str  # Add Monthly RSI column
        }
                
        
    except Exception as e:
        print(f'Error in data fetch for {symInfo.instrument_key}: {e}')
        return None

def process_data(df):
    results = []
    for _, row in df.iterrows():
        result = getHistoricalData(row)
        if result:
            results.append(result)
    return pd.DataFrame(results)


# Process data for Nifty 50 and Nifty 100 separately
nifty_50_results_df = process_data(nifty_50_df)
nifty_100_results_df = process_data(nifty_100_df)
nifty_200_results_df = process_data(nifty_200_df)


# Authenticate with Google Sheets
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name(r'credentials.json', scope)
client = gspread.authorize(creds)

def update_sheet(file_name, df, sheet_name):
    try:
        # Open the main spreadsheet
        spreadsheet = client.open(file_name)

        # Open the specific worksheet
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            print(f"Worksheet '{sheet_name}' not found in '{file_name}'.")
            return

        # Define start row and end row for the data
        start_row = 4
        end_row = len(df) + start_row - 1

        # Clear the existing data from row 5 to the end in columns A to P
        try:
            clear_range = f'A{start_row}:P{end_row}'
            worksheet.batch_clear([clear_range])
            print(f"Cleared data in range {clear_range}.")
        except gspread.exceptions.APIError as e:
            print(f"API Error clearing range in '{sheet_name}': {e}")

        # Update the data (including headers if needed) starting from row 5
        try:
            worksheet.update(range_name=f'A{start_row}', values=df.values.tolist())
            print(f"Data successfully updated in '{sheet_name}' worksheet.")
        except gspread.exceptions.APIError as e:
            print(f"API Error updating '{sheet_name}': {e}")

        # # Clear column H from row 5 to the end
        # try:
        #     clear_col_h_range = f'H{start_row}:H{end_row}'
        #     worksheet.batch_clear([clear_col_h_range])
        #     print(f"Cleared data in column H from H{start_row} to H{end_row}.")
        # except gspread.exceptions.APIError as e:
        #     print(f"API Error clearing column H in '{sheet_name}': {e}")
        # Update cell I1 with the current timestamp in 12-hour format
        try:
            ist = pytz.timezone('Asia/Kolkata')
            update_timestamp = datetime.now(ist).strftime("Last Update: %d-%m-%Y %I:%M:%S %p")
            worksheet.update('I1', [[update_timestamp]])  # Note the double brackets
            print(f"Updated cell I1 with timestamp '{update_timestamp}'.")
        except gspread.exceptions.APIError as e:
            print(f"API Error updating cell I1 in '{sheet_name}': {e}")

    except gspread.exceptions.SpreadsheetNotFound:
        print(f"Spreadsheet '{file_name}' not found.")





if __name__ == "__main__":
    update_sheet("SST WITH RSI AND RS  BY MILAN YFINACE", process_data(nifty_50_df), "SST-N50")
    update_sheet("SST WITH RSI AND RS  BY MILAN YFINACE", process_data(nifty_100_df), "SST-N100")
    update_sheet("SST WITH RSI AND RS  BY MILAN YFINACE", process_data(nifty_200_df), "SST-N200")
