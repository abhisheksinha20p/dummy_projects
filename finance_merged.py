import pandas as pd
import requests
import yfinance as yf
import sys

def get_sp500_tickers_wikipedia():
    """Get S&P 500 tickers from Wikipedia"""
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    try:
        tables = pd.read_html(url)
        sp500_table = tables[0]  # First table on the page contains the tickers
        tickers = sp500_table['Symbol'].tolist()
        return tickers
    except ImportError as e:
        print(f"Error: Missing dependency. {e}")
        print("Please install required packages: pip install lxml")
        return []
    except (requests.RequestException, ValueError) as e:
        print(f"Error fetching S&P 500 tickers: {e}")
        return []

def fetch_stock_data(tickers):
    # Process tickers in smaller batches to avoid API limits
    batch_size = 50
    all_data = {}
    
    for i in range(0, len(tickers), batch_size):
        batch_tickers = tickers[i:i+batch_size]
        print(f"Processing batch {i//batch_size + 1}/{(len(tickers)-1)//batch_size + 1} ({len(batch_tickers)} tickers)...")
        
        data = yf.Tickers(" ".join(batch_tickers))
        
        for ticker in batch_tickers:
            try:
                stock = data.tickers[ticker]
                info = stock.info
                hist = stock.history(period="1y", interval="1d", auto_adjust=False)
                
                if len(hist) >= 200:
                    # Moving Averages
                    hist['50D MA'] = hist['Close'].rolling(window=50).mean()
                    hist['200D MA'] = hist['Close'].rolling(window=200).mean()
                    
                    # MACD
                    ema12 = hist['Close'].ewm(span=12, adjust=True).mean()
                    ema26 = hist['Close'].ewm(span=26, adjust=True).mean()
                    hist['MACD Line'] = ema12 - ema26
                    hist['MACD Signal'] = hist['MACD Line'].ewm(span=9, adjust=False).mean()
                    hist['MACD Hist'] = hist['MACD Line'] - hist['MACD Signal']
                    
                    # RSI
                    delta = hist['Close'].diff()
                    gain = delta.where(delta > 0, 0)
                    loss = -delta.where(delta < 0, 0)
                    avg_gain = gain.rolling(window=14).mean()
                    avg_loss = loss.rolling(window=14).mean()
                    rs = avg_gain / avg_loss
                    hist['RSI'] = 100 - (100 / (1 + rs))
                    
                    # Bollinger Bands
                    hist['BB Middle'] = hist['Close'].rolling(window=20).mean()
                    bb_std = hist['Close'].rolling(window=20).std()
                    hist['BB Upper'] = hist['BB Middle'] + 2 * bb_std
                    hist['BB Lower'] = hist['BB Middle'] - 2 * bb_std
                    
                    # Stochastic Oscillator
                    low_14 = hist['Low'].rolling(window=14).min()
                    high_14 = hist['High'].rolling(window=14).max()
                    hist['Stoch %K'] = 100 * (hist['Close'] - low_14) / (high_14 - low_14)
                    hist['Stoch %D'] = hist['Stoch %K'].rolling(window=3).mean()
                    
                    # ATR
                    high_low = hist['High'] - hist['Low']
                    high_close = (hist['High'] - hist['Close'].shift()).abs()
                    low_close = (hist['Low'] - hist['Close'].shift()).abs()
                    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
                    hist['ATR'] = tr.rolling(window=14).mean()
                    
                    # Add ticker column and info
                    hist['Ticker'] = ticker
                    hist['Sector'] = info.get('sector')
                    hist['Industry'] = info.get('industry')
                    
                    # Get the last 5 days of data
                    recent_hist = hist.tail(5).reset_index()
                    
                    # Convert timezone-aware datetime to timezone-naive
                    if 'Date' in recent_hist.columns and hasattr(recent_hist['Date'].dtype, 'tz'):
                        recent_hist['Date'] = recent_hist['Date'].dt.tz_localize(None)
                    
                    all_data[ticker] = recent_hist
                else:
                    print(f"[!] Not enough data to compute 200D MA for {ticker}")
            except Exception as e:
                print(f"Error processing {ticker}: {e}")
                continue
    
    return all_data

# Get S&P 500 tickers
print("Fetching S&P 500 tickers from Wikipedia...")
sp500_tickers = get_sp500_tickers_wikipedia()
print(f"S&P 500 Tickers ({len(sp500_tickers)}):")
print(f"First 5 tickers: {sp500_tickers[:5]} ...")

# Save tickers to CSV
if sp500_tickers:
    pd.DataFrame(sp500_tickers, columns=['Ticker']).to_csv('sp500_tickers.csv', index=False)
    print("S&P 500 tickers saved to sp500_tickers.csv")

    print(f"\nProcessing all {len(sp500_tickers)} tickers for detailed analysis...")
    
    # Fetch and analyze stock data for all tickers
    ticker_data = fetch_stock_data(sp500_tickers)
    
    print(f"Successfully processed {len(ticker_data)} tickers")
    
    # Save to Excel with multiple sheets
    print("Saving data to Excel file...")
    with pd.ExcelWriter("sp500_analysis.xlsx", engine='openpyxl') as writer:
        # Create a summary sheet with the latest data
        summary_data = []
        for ticker, hist_df in ticker_data.items():
            if not hist_df.empty:
                latest = hist_df.iloc[-1].to_dict()
                summary_data.append(latest)
        
        if summary_data:
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
        
        # Save each ticker's data to a separate sheet
        for ticker, hist_df in ticker_data.items():
            if not hist_df.empty:
                sheet_name = f"{ticker}"[:31]  # Excel sheet names limited to 31 chars
                hist_df.to_excel(writer, sheet_name=sheet_name, index=False)
    
    print("Data saved to sp500_analysis.xlsx with multiple sheets")
else:
    print("No tickers retrieved. Please check dependencies and try again.")