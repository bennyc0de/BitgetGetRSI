from pybitget import Client
import logging
import pandas as pd
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# API credentials (replace with your actual keys)
APIKEY = 'api'
SECRETKEY = 'key'
PASSPHRASE = 'pass'

try:
    # Initialize the Bitget client
    client = Client(APIKEY, SECRETKEY, passphrase=PASSPHRASE)
    logging.info("Successfully initialized Bitget client")

    # Fetch futures market tickers (USDT-margined perpetual futures)
    tickers = client.mix_get_all_symbol_ticker('umcbl')
    if 'data' not in tickers or not tickers['data']:
        logging.warning("No ticker data received from Bitget")
        print({})
        exit()

    # Print ticker data to verify volume field
    # print([tickers['data']])

    # Filter for high-volume tickers (usdtVol > 500,000, lowered threshold)
    high_volume_tickers = [
        ticker for ticker in tickers['data']
        if float(ticker.get('usdtVolume', 0)) > 5_000_000
    ]

    if not high_volume_tickers:
        logging.info("No high-volume tickers found")
        print({})
        exit()

    # Function to fetch historical candlestick data for a symbol
    def get_historical_data(symbol, interval, limit=15):
        if interval == '1D':
            interval_seconds = 86400  # 1 day in seconds
        elif interval == '1m':
            interval_seconds = 60  # 4 hours in seconds
        else:
            interval_seconds = 3600  # Default to 1 hour
        end_time = int(datetime.now().timestamp() * 1000)  # Current time in milliseconds
        start_time = end_time - (limit * interval_seconds * 1000)  # Adjust for interval
        logging.info(f"Fetching candles for {symbol} with interval {interval}, start {start_time}, end {end_time}")
        candles = client.mix_get_candles(symbol, interval, start_time, end_time)
        if not candles:
            logging.warning(f"No candle data returned for {symbol}")
            return None
        #print(candles)
        df = pd.DataFrame(candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'baseVolume', 'quoteVolume'])
        df['close'] = df['close'].astype(float)
        #logging.info(f"Candle data for {symbol}: {df.shape} rows, Close prices: {df['close'].head()}")
        if not df['close'].isna().any() and (df['close'] > 0).all() and len(df) >= 14:
            return df
        logging.warning(f"Invalid or insufficient close prices for {symbol}: NaN/zero values or fewer than 14 rows")
        return None

    # Function to calculate RSI
    def calculate_rsi(df, period=15):  
        if df is None or len(df) < period:
            logging.warning("DataFrame is None or too short for RSI calculation")
            return None
        try:
            close_series = pd.Series(df['close'].values, dtype=float)
            if close_series.isna().any() or (close_series <= 0).any():
                logging.warning("Close prices contain NaN or non-positive values")
                return None
            # Calculate price changes (deltas)
            delta = close_series.diff()
            delta = delta.iloc[1:]
            # Separate gains and losses
            gains = delta.where(delta > 0, 0)
            losses = -delta.where(delta < 0, 0)
            
            # Calculate average gain and loss over 14 periods (first calculation uses the full period)
            period = 14
            avg_gain = gains.rolling(window=period, min_periods=1).mean().iloc[period-1]  
            avg_loss = losses.rolling(window=period, min_periods=1).mean().iloc[period-1]  

            # Handle division by zero or NaN
            if avg_loss == 0:
                rs = float('inf') if avg_gain > 0 else 0
            else:
                rs = avg_gain / avg_loss

            # Calculate RSI
            rsi = 100 - (100 / (1 + rs)) if rs != float('inf') else 100
            
            # Get the most recent RSI value, ensuring it’s not NaN
            if not rsi:
                logging.warning("RSI calculation returned NaN or insufficient data")
                return None
            
            logging.info(f"Calculated RSI for {df['close'].name}: {rsi}")
            return rsi
        except Exception as e:
            logging.error(f"Error calculating RSI: {e}")
            return None

    # Filter for tickers with high or low RSI
    rsi_filtered_tickers = {}
    for ticker in high_volume_tickers:
        symbol = ticker['symbol']
        logging.info(f"Fetching data and calculating RSI for {symbol}")
        
        # Fetch historical data
        df = get_historical_data(symbol, '1H', 15) 
        if df is not None:
            # Calculate RSI
            rsi = calculate_rsi(df)
            if rsi is not None:
                logging.info(f"Symbol: {symbol}, RSI: {round(rsi, 2)}, USDT Volume: {ticker.get('usdtVolume', 0)}")
                # Check if RSI is above 70 (overbought) or below 30 (oversold)
                if rsi > 70 or rsi < 30:
                    rsi_filtered_tickers[symbol] = {
                        'usdtVol': ticker.get('usdtVolume', 0),
                        'rsi': round(rsi, 2)
                    }
    sorted_rsi_filtered_tickers = dict(sorted(rsi_filtered_tickers.items(), key=lambda item: item[1]['rsi']))
    # Print the filtered results
    if sorted_rsi_filtered_tickers:
        logging.info(f"Found {len(sorted_rsi_filtered_tickers)} tickers with RSI > 70 or < 30")
        for symbol, details in sorted_rsi_filtered_tickers.items():
            print(f"Symbol: {symbol}, USDT Volume: {details['usdtVol']}, RSI: {details['rsi']}")
    else:
        logging.info("No tickers found with RSI > 70 or < 30")
        print({})

except Exception as e:
    logging.error(f"Error: {e}")
    print({})