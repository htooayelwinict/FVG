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

    def load_candles(self, symbol: str = 'BTCUSDT', timeframe: str = 'D1') -> List[Dict[str, Any]]:
        """Load 500 candles from current time"""
        try:
            interval = self.timeframe_mapping.get(timeframe)
            if not interval:
                raise ValueError(f"Invalid timeframe: {timeframe}")

            # Get current UTC time
            current_time = datetime.utcnow()
            end_ts = int(current_time.timestamp() * 1000)

            # Fetch 500 klines from Binance
            klines = self.client.get_historical_klines(
                symbol=symbol,
                interval=interval,
                end_str=end_ts,
                limit=200
            )

            if not klines:
                return []

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

            logger.debug(f"Loaded {len(data)} candles")
            return data

        except Exception as e:
            logger.error(f"Error fetching data: {str(e)}")
            return []

    def get_latest_price(self, symbol: str) -> float:
        """Get the latest price directly from Binance API"""
        try:
            ticker = self.client.get_symbol_ticker(symbol=symbol)
            price = float(ticker['price'])
            logger.debug(f"Latest price for {symbol}: {price}")
            return price
        except Exception as e:
            logger.error(f"Error fetching price: {str(e)}", exc_info=True)
            return None