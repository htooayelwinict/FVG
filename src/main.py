import os
import sys
from pathlib import Path
import json
import pandas as pd
from datetime import datetime, timedelta, UTC
from blessed import Terminal
import time
from signal import signal, SIGINT
from datetime import timezone
import threading
from queue import Queue
import asyncio
import logging

# Add the src directory to the Python path
current_dir = Path(__file__).parent
project_root = current_dir.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from src.backtest.detect_engine import DetectionEngine
from src.utils.data_loader import BinanceDataLoader
from src.utils.display_manager import DisplayManager
from src.utils.logger import setup_logger

# Initialize blessed terminal
term = Terminal()

logger = setup_logger('fvg_monitor')

class FVGMonitor:
    def __init__(self):
        logger.debug("Initializing FVG Monitor")
        self.running = True
        self.data_loader = BinanceDataLoader()
        self.display_manager = DisplayManager()
        self.bullish_fvgs = []
        self.bearish_fvgs = []
        self.latest_price = None
        self.symbol = None
        self.timeframe = None
        self.last_update = None
        self.last_fvg_update = None
        
        # Thread-safe queues for communication
        self.fvg_queue = Queue()
        self.price_queue = Queue()
        
        # Add detector instance
        self.detector = None

    def initialize_detector(self):
        """Initialize detection engine with current settings"""
        virtual_data_path = f"binance://{self.symbol}_{self.timeframe}"
        self.detector = DetectionEngine(
            data_source=virtual_data_path,
            timeframe=self.timeframe
        )

    def detection_thread(self):
        """Thread for FVG detection"""
        logger.debug("Starting detection thread")
        while self.running:
            try:
                current_time = datetime.now(UTC)
                logger.debug(f"Detection cycle at {current_time}")
                
                # Collect new FVGs
                results = self.detector.detect_fvgs()
                if results:
                    self.fvg_queue.put({
                        'bullish': results['bullish_fvgs'],
                        'bearish': results['bearish_fvgs'],
                        'time': current_time
                    })
                    self.last_fvg_update = current_time
                
                time.sleep(0.2)  # Check every 0.2 seconds
                
            except Exception as e:
                logger.error(f"Detection thread error: {str(e)}", exc_info=True)
                time.sleep(1)

    def price_thread(self):
        """Thread for price updates"""
        consecutive_errors = 0
        while self.running:
            try:
                current_time = datetime.now(UTC)
                price = self.data_loader.get_latest_price(self.symbol)
                if price:
                    self.price_queue.put({
                        'price': price,
                        'time': current_time.strftime('%Y-%m-%d %H:%M:%S UTC')
                    })
                    consecutive_errors = 0
                    time.sleep(0.1)
                else:
                    consecutive_errors += 1
                    time.sleep(min(0.2 * (1.5 ** consecutive_errors), 2.0))
                    
            except Exception as e:
                logger.error(f"Price thread error: {str(e)}", exc_info=True)
                consecutive_errors += 1
                time.sleep(min(0.2 * (1.5 ** consecutive_errors), 2.0))

    def display_thread(self):
        """Thread for display updates"""
        while self.running:
            try:
                current_time = datetime.now(UTC)
                update_needed = False
                
                # Process all queued data at once
                fvg_updates = []
                price_updates = []
                
                # Batch process FVG updates
                while not self.fvg_queue.empty():
                    fvg_data = self.fvg_queue.get()
                    fvg_updates.append(fvg_data)
                
                # Use most recent FVG update
                if fvg_updates:
                    latest_fvg = fvg_updates[-1]
                    self.bullish_fvgs = latest_fvg['bullish']
                    self.bearish_fvgs = latest_fvg['bearish']
                    self.last_fvg_update = latest_fvg['time']
                    update_needed = True
                
                # Batch process price updates
                while not self.price_queue.empty():
                    price_data = self.price_queue.get()
                    price_updates.append(price_data)
                
                # Use most recent price update
                if price_updates:
                    latest_price = price_updates[-1]
                    self.latest_price = latest_price['price']
                    self.last_update = latest_price['time']
                    update_needed = True
                
                # Update display when new data arrives
                if update_needed and self.latest_price and (self.bullish_fvgs or self.bearish_fvgs):
                    self.display_manager.update_screen(
                        symbol=self.symbol,
                        timeframe=self.timeframe,
                        latest_price=self.latest_price,
                        last_update=self.last_update,
                        bullish_fvgs=self.bullish_fvgs,
                        bearish_fvgs=self.bearish_fvgs
                    )
                
                time.sleep(0.05)
                
            except Exception as e:
                logger.error(f"Display thread error: {str(e)}", exc_info=True)
                time.sleep(0.1)

    def handle_exit(self, signum, frame):
        """Handle exit signal"""
        self.running = False
        print("\nShutting down...")
        time.sleep(1)
        print(term.normal + term.show_cursor)
        sys.exit(0)

    def monitor_fvgs(self, symbol: str, timeframe: str):
        """Main monitoring loop"""
        self.symbol = symbol
        self.timeframe = timeframe
        signal(SIGINT, self.handle_exit)
        
        # Initialize detector
        self.initialize_detector()

        print(term.clear + term.hide_cursor)
        
        try:
            # Start threads
            threads = [
                threading.Thread(target=self.detection_thread, daemon=True),
                threading.Thread(target=self.price_thread, daemon=True),
                threading.Thread(target=self.display_thread, daemon=True)
            ]
            
            for thread in threads:
                thread.start()
            
            # Wait for threads
            while self.running:
                time.sleep(0.1)
                
        except Exception as e:
            logger.error(f"Main thread error: {str(e)}", exc_info=True)
            
        finally:
            self.running = False
            print(term.normal + term.show_cursor)

def main():
    """Main function to run FVG monitor"""
    monitor = FVGMonitor()
    
    print("Starting FVG Monitor...")
    
    # Get user input
    timeframe = input("Enter timeframe (D1, H4, H1, M15, M5, M3, M1): ").strip().upper()
    while timeframe not in ['D1', 'H4', 'H1', 'M15', 'M5', 'M3', 'M1']:
        print("Invalid timeframe. Please choose from D1, H4, H1, M15, M5, M3, M1.")
        timeframe = input("Enter timeframe: ").strip().upper()
    
    symbol = input("Enter trading pair (default: BTCUSDT): ").strip().upper() or "BTCUSDT"
    
    # Start monitoring
    monitor.monitor_fvgs(symbol, timeframe)

if __name__ == "__main__":
    main()