import csv
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path
import pandas as pd
from datetime import datetime
import numpy as np
from binance.client import Client
from binance.exceptions import BinanceAPIException
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from src.utils.logger import setup_logger

load_dotenv()
logger = setup_logger('data_loader')

class BinanceDataLoader:
    def __init__(self):
        logger.debug("Initializing BinanceDataLoader")
        # Initialize Binance client
        # Get API keys from environment variables for security
        api_key = os.getenv('BINANCE_API_KEY')
        api_secret = os.getenv('BINANCE_API_SECRET')
        
        if not api_key or not api_secret:
            raise ValueError("Please set BINANCE_API_KEY and BINANCE_API_SECRET environment variables")
        
        self.client = Client(api_key, api_secret)
        
        # Mapping for timeframe strings
        self.timeframe_mapping = {
            'M1': Client.KLINE_INTERVAL_1MINUTE,
            'M3': Client.KLINE_INTERVAL_3MINUTE,
            'M5': Client.KLINE_INTERVAL_5MINUTE,
            'M15': Client.KLINE_INTERVAL_15MINUTE,
            'M30': Client.KLINE_INTERVAL_30MINUTE,
            'H1': Client.KLINE_INTERVAL_1HOUR,
            'H4': Client.KLINE_INTERVAL_4HOUR,
            'D1': Client.KLINE_INTERVAL_1DAY,
        }
        
        # Add timeframe-specific batch sizes
        self.timeframe_batch_sizes = {
            'M1': 100,   # 100 minutes
            'M3': 100,   # 300 minutes
            'M5': 100,   # 500 minutes
            'M15': 100,  # 1500 minutes
            'M30': 100,  # 3000 minutes
            'H1': 100,   # 100 hours
            'H4': 300,   # 1200 hours (similar to daily for better coverage)
            'D1': 300,   # 300 days for daily
        }

    def load_candlestick_data(self, 
                             symbol: str = 'BTCUSDT',
                             timeframe: str = 'M15',
                             start_date: str = None,
                             end_date: str = None) -> Tuple[List[Dict[str, Any]], int]:
        """
        Load candlestick data from Binance API.
        """
        try:
            interval = self.timeframe_mapping.get(timeframe)
            if not interval:
                raise ValueError(f"Invalid timeframe: {timeframe}")

            # Convert dates to timestamps
            if start_date:
                start_ts = int(pd.to_datetime(start_date).timestamp() * 1000)
            else:
                # Default to 4 hours ago if no start date
                start_ts = int((datetime.utcnow() - timedelta(hours=4)).timestamp() * 1000)
                
            if end_date:
                end_ts = int(pd.to_datetime(end_date).timestamp() * 1000)
            else:
                # Default to current time if no end date
                end_ts = int(datetime.utcnow().timestamp() * 1000)

            # Fetch klines from Binance
            klines = self.client.get_historical_klines(
                symbol=symbol,
                interval=interval,
                start_str=start_ts,
                end_str=end_ts,
                limit=1000
            )

            if not klines:
                print(f"No data received for {symbol} {timeframe}")
                return [], 0

            # Convert to DataFrame
            df = pd.DataFrame(klines, columns=[
                'Time', 'Open', 'High', 'Low', 'Close', 'Volume',
                'Close_time', 'Quote_volume', 'Trades', 'Taker_buy_base',
                'Taker_buy_quote', 'Ignore'
            ])

            # Convert types
            df['Time'] = pd.to_datetime(df['Time'], unit='ms', utc=True)
            numeric_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
            for col in numeric_columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

            original_total_candles = len(df)
            print(f"Data range: {df['Time'].min()} to {df['Time'].max()}")

            # Sort in reverse order (newest first)
            df = df.sort_values('Time', ascending=False)

            # Convert to dictionary format
            data = []
            for _, row in df.iterrows():
                candle = {
                    'Time': row['Time'].strftime('%Y-%m-%d %H:%M:%S UTC'),
                    'Open': float(row['Open']),
                    'High': float(row['High']),
                    'Low': float(row['Low']),
                    'Close': float(row['Close']),
                    'Volume': float(row['Volume'])
                }
                data.append(candle)

            print(f"Successfully loaded {len(data)} candles")
            return data, original_total_candles

        except BinanceAPIException as e:
            print(f"Binance API error: {str(e)}")
            return [], 0
        except Exception as e:
            print(f"Error fetching data: {str(e)}")
            return [], 0

    def load_candlestick_batch(self, 
                              symbol: str = 'BTCUSDT',
                              timeframe: str = 'M15',
                              start_date: str = None,
                              batch_size: int = 50) -> Tuple[List[Dict[str, Any]], str, bool]:
        """Load a batch of candles with timeframe-specific adjustments"""
        try:
            interval = self.timeframe_mapping.get(timeframe)
            if not interval:
                raise ValueError(f"Invalid timeframe: {timeframe}")
                
            # Adjust batch size based on timeframe
            adjusted_batch_size = self.timeframe_batch_sizes.get(timeframe, batch_size)
            
            # Get current UTC time
            current_time = datetime.utcnow()
            
            # If no start_date provided, calculate the end of the last completed candle
            if not start_date:
                # Calculate timeframe in minutes
                if 'M' in timeframe:
                    minutes = int(timeframe.replace('M', ''))
                elif 'H' in timeframe:
                    minutes = int(timeframe.replace('H', '')) * 60
                elif 'D' in timeframe:
                    minutes = int(timeframe.replace('D', '')) * 1440
                    
                # Calculate the start of the current candle in UTC
                minutes_elapsed = current_time.minute + current_time.hour * 60
                current_candle_start = minutes_elapsed - (minutes_elapsed % minutes)
                
                # Get the end of the last completed candle
                last_candle_end = current_time.replace(
                    minute=current_candle_start - 1,
                    second=59,
                    microsecond=999999
                )
                end_ts = int(last_candle_end.timestamp() * 1000)
            else:
                # Handle start_date properly with timezone
                try:
                    # If start_date is already a UTC timestamp string
                    if 'UTC' in start_date:
                        parsed_date = pd.to_datetime(start_date.replace(' UTC', ''))
                    else:
                        parsed_date = pd.to_datetime(start_date)
                    
                    # Ensure timezone is UTC
                    if parsed_date.tzinfo is None:
                        parsed_date = parsed_date.tz_localize('UTC')
                    else:
                        parsed_date = parsed_date.tz_convert('UTC')
                    
                    end_ts = int(parsed_date.timestamp() * 1000)
                except Exception as e:
                    print(f"Error parsing date: {e}")
                    return [], None, False

            # Fetch klines from Binance
            klines = self.client.get_historical_klines(
                symbol=symbol,
                interval=interval,
                end_str=end_ts,
                limit=adjusted_batch_size + 1
            )

            if not klines:
                return [], None, False

            # Convert to DataFrame
            df = pd.DataFrame(klines, columns=[
                'Time', 'Open', 'High', 'Low', 'Close', 'Volume',
                'Close_time', 'Quote_volume', 'Trades', 'Taker_buy_base',
                'Taker_buy_quote', 'Ignore'
            ])

            # Convert types and ensure UTC timezone
            df['Time'] = pd.to_datetime(df['Time'], unit='ms', utc=True)
            numeric_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
            for col in numeric_columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

            # Sort in reverse chronological order (newest first)
            df = df.sort_values('Time', ascending=False)
            
            # Take only batch_size candles
            df = df.head(adjusted_batch_size)

            # Convert to dictionary format with UTC times
            data = []
            for _, row in df.iterrows():
                candle = {
                    'Time': row['Time'].strftime('%Y-%m-%d %H:%M:%S UTC'),
                    'Open': float(row['Open']),
                    'High': float(row['High']),
                    'Low': float(row['Low']),
                    'Close': float(row['Close']),
                    'Volume': float(row['Volume'])
                }
                data.append(candle)

            # Get the timestamp of the oldest candle in this batch for next batch
            last_date = data[-1]['Time'] if data else None
            
            # Check if there's more historical data
            has_more = len(klines) > adjusted_batch_size

            # print(f"Loaded batch of {len(data)} candles, newest: {data[0]['Time']}, oldest: {last_date}")
            return data, last_date, has_more

        except Exception as e:
            print(f"Error fetching batch: {str(e)}")
            return [], None, False

    def get_latest_price(self, symbol: str) -> float:
        """Get the latest price directly from Binance API"""
        try:
            # Get ticker price for the symbol
            ticker = self.client.get_symbol_ticker(symbol=symbol)
            price = float(ticker['price'])
            logger.debug(f"Latest price for {symbol}: {price}")
            return price
        except Exception as e:
            logger.error(f"Error fetching price: {str(e)}", exc_info=True)
            return None

def convert_timestamp_to_iso(timestamp: Any) -> str:
    """Convert various timestamp formats to ISO format"""
    try:
        # Handle numeric timestamps
        if isinstance(timestamp, (int, float)):
            ts = float(timestamp)
            # Convert to milliseconds if needed
            if ts > 1e16:  # microseconds
                ts /= 1000
            elif ts < 1e12:  # seconds
                ts *= 1000
            return pd.Timestamp(ts, unit='ms').strftime('%Y-%m-%d %H:%M:%S')
        
        # Handle string timestamps
        return pd.to_datetime(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        raise ValueError(f"Unable to parse timestamp: {timestamp}")