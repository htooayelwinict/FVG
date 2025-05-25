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
from datetime import timezone

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
        self.server_time_offset = self._get_time_offset()
        
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

        # Timeframe configurations
        self.timeframe_config = {
            'M1': {'candle_ms': 60 * 1000},
            'M3': {'candle_ms': 3 * 60 * 1000},
            'M5': {'candle_ms': 5 * 60 * 1000},
            'M15': {'candle_ms': 15 * 60 * 1000},
            'M30': {'candle_ms': 30 * 60 * 1000},
            'H1': {'candle_ms': 60 * 60 * 1000},
            'H4': {'candle_ms': 4 * 60 * 60 * 1000},
            'D1': {'candle_ms': 24 * 60 * 60 * 1000},
        }

    def _get_time_offset(self) -> int:
        """Get the time offset between local and server time"""
        server_time = self.client.get_server_time()
        local_time = int(datetime.utcnow().timestamp() * 1000)
        return server_time['serverTime'] - local_time

    def _get_current_server_time(self) -> int:
        """Get current server time in milliseconds"""
        local_time = int(datetime.utcnow().timestamp() * 1000)
        return local_time + self.server_time_offset

    def _get_last_forming_candle_time(self, timeframe: str) -> int:
        """Calculate the timestamp of the last completed candle"""
        server_time = self._get_current_server_time()
        candle_ms = self.timeframe_config[timeframe]['candle_ms']
        
        # Round down to get the last completed candle
        last_completed_candle_ms = (server_time // candle_ms) * candle_ms
        
        return last_completed_candle_ms

    def load_candles(self, symbol: str = 'BTCUSDT', timeframe: str = 'M15') -> List[Dict[str, Any]]:
        """Load 200 candles ending at the last completed candle"""
        try:
            interval = self.timeframe_mapping.get(timeframe)
            if not interval:
                raise ValueError(f"Invalid timeframe: {timeframe}")

            # Get the current forming candle time
            end_ts = self._get_last_forming_candle_time(timeframe)
            
            # Fetch klines from Binance
            klines = self.client.get_historical_klines(
                symbol=symbol,
                interval=interval,
                end_str=end_ts,
                limit=100
            )

            if not klines:
                return []

            # Direct conversion to dictionary without DataFrame
            # Process in reverse order (newest first) during creation
            data = []
            for kline in reversed(klines):
                data.append({
                    'Time': datetime.fromtimestamp(kline[0]/1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC'),
                    'Open': float(kline[1]),
                    'High': float(kline[2]),
                    'Low': float(kline[3]),
                    'Close': float(kline[4]),
                    'Volume': float(kline[5])
                })

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