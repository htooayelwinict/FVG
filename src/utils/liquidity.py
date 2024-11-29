from binance.client import Client
from binance.streams import BinanceSocketManager
from datetime import datetime, timezone, timedelta
import asyncio
import json
import os
from typing import List, Dict, Any, Optional, Tuple
import pandas as pd
from binance.exceptions import BinanceAPIException
import numpy as np
from logger import setup_logger
from dotenv import load_dotenv
from collections import defaultdict

# Load environment variables
load_dotenv()
logger = setup_logger('liquidity_analyzer')

class MarketDataAnalyzer:
    def __init__(self):
        """Initialize analyzer with data structures"""
        logger.debug("Initializing MarketDataAnalyzer")
        
        # Get API keys from environment
        api_key = os.getenv('BINANCE_API_KEY')
        api_secret = os.getenv('BINANCE_API_SECRET')
        
        if not api_key or not api_secret:
            raise ValueError("API keys not found in environment variables")
            
        self.client = Client(api_key, api_secret)
        logger.info("Successfully initialized Binance client")
        
        # Initialize data structures
        self.trade_history = {
            'bids': defaultdict(list),  # Price -> List of trade data
            'asks': defaultdict(list)
        }
        self.order_filled = {
            'bids': defaultdict(dict),
            'asks': defaultdict(dict)
        }
        self.orderbook = {
            'bids': {},  # Price -> Quantity
            'asks': {}
        }
        self.historical_data_loaded = False
        self.last_update_id = None
        self.cleanup_threshold = timedelta(hours=24)
        self.message_counter = 0
        
    async def initialize_historical_data(self, symbol: str):
        """Load 24-hour historical trade data"""
        try:
            logger.info(f"Loading 24-hour trade history for {symbol}")
            
            # Get current time in UTC
            current_time = datetime.now(tz=timezone.utc)
            start_time = current_time - timedelta(hours=24)
            
            # Convert to millisecond timestamps
            end_ms = int(current_time.timestamp() * 1000)
            start_ms = int(start_time.timestamp() * 1000)
            
            # Get recent trades using recent trades endpoint
            trades = self.client.get_recent_trades(
                symbol=symbol,
                limit=1000  # Maximum allowed
            )
            
            # Filter trades within our time window
            valid_trades = []
            for trade in trades:
                trade_time = int(trade['time'])
                if start_ms <= trade_time <= end_ms:
                    valid_trades.append(trade)
            
            logger.info(f"Retrieved {len(valid_trades)} valid trades within the last 24 hours")
            
            # Process the valid trades
            for trade in valid_trades:
                price = float(trade['price'])
                quantity = float(trade['qty'])
                is_buyer = trade['isBuyerMaker']
                trade_time = int(trade['time'])
                
                trade_data = {
                    'quantity': quantity,
                    'timestamp': datetime.fromtimestamp(trade_time / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC'),
                    'trade_id': trade['id']
                }
                
                # Add to appropriate list
                if is_buyer:
                    self.trade_history['bids'][price].append(trade_data)
                else:
                    self.trade_history['asks'][price].append(trade_data)
            
            self.historical_data_loaded = True
            logger.info(f"Successfully processed {len(valid_trades)} historical trades")
            
            # Initialize order_filled with historical data
            await self._update_order_filled()
            
        except Exception as e:
            logger.error(f"Error loading historical data: {str(e)}")
            raise  # Re-raise to see full error in logs
            
    async def verify_historical_data(self) -> bool:
        """Verify the integrity of historical data"""
        try:
            if not self.historical_data_loaded:
                logger.warning("Historical data not loaded yet")
                return False

            # Check if we have both bids and asks
            if not self.trade_history['bids'] or not self.trade_history['asks']:
                logger.warning("Missing bid or ask historical data")
                return False

            # Verify timestamps are within expected range
            now = datetime.now(tz=timezone.utc)
            cutoff_time = now - timedelta(days=1)
            
            for side in ['bids', 'asks']:
                for price, trades in self.trade_history[side].items():
                    for trade in trades:
                        trade_time = datetime.strptime(trade['timestamp'], '%Y-%m-%d %H:%M:%S UTC').replace(tzinfo=timezone.utc)
                        if trade_time < cutoff_time:
                            logger.warning(f"Found outdated trade data: {trade['timestamp']}")
                            return False

            logger.info("Historical data verification passed")
            return True

        except Exception as e:
            logger.error(f"Error verifying historical data: {str(e)}")
            return False

    async def cleanup_old_data(self):
        """Remove trade data older than cleanup_threshold"""
        try:
            now = datetime.now(tz=timezone.utc)
            cleaned = 0

            for side in ['bids', 'asks']:
                for price in list(self.trade_history[side].keys()):
                    trades = self.trade_history[side][price]
                    updated_trades = []

                    for trade in trades:
                        trade_time = datetime.strptime(trade['timestamp'], '%Y-%m-%d %H:%M:%S UTC').replace(tzinfo=timezone.utc)
                        if now - trade_time <= self.cleanup_threshold:
                            updated_trades.append(trade)
                        else:
                            cleaned += 1

                    if updated_trades:
                        self.trade_history[side][price] = updated_trades
                    else:
                        del self.trade_history[side][price]

            if cleaned > 0:
                logger.info(f"Cleaned up {cleaned} old trade records")

        except Exception as e:
            logger.error(f"Error during data cleanup: {str(e)}")

    async def _update_order_filled(self):
        """Update order_filled data structure"""
        if not self.historical_data_loaded:
            return
            
        # Process bids
        for price, trades in self.trade_history['bids'].items():
            self.order_filled['bids'][price] = {
                'total_quantity': sum(t['quantity'] for t in trades),
                'trade_count': len(trades),
                'last_update': max(t['timestamp'] for t in trades)
            }
            
        # Process asks
        for price, trades in self.trade_history['asks'].items():
            self.order_filled['asks'][price] = {
                'total_quantity': sum(t['quantity'] for t in trades),
                'trade_count': len(trades),
                'last_update': max(t['timestamp'] for t in trades)
            }
            
    async def start_market_stream(self, symbol: str = 'btcusdt'):
        """Start WebSocket stream for market data"""
        logger.info(f"Starting market stream for {symbol}")
        
        while True:  # Main reconnection loop
            try:
                # First load historical data
                await self.initialize_historical_data(symbol.upper())
                
                # Verify historical data
                if not await self.verify_historical_data():
                    logger.error("Historical data verification failed")
                    await asyncio.sleep(5)  # Wait before retry
                    continue
                
                # Start WebSocket stream
                bm = BinanceSocketManager(self.client)
                
                # Get initial order book snapshot
                depth = await self._get_order_book_snapshot(symbol.upper())
                self.last_update_id = depth['lastUpdateId']
                
                # Combine trade and depth streams
                async with bm.multiplex_socket([
                    f"{symbol}@depth@100ms",
                    f"{symbol}@trade"
                ]) as stream:
                    cleanup_counter = 0
                    message_counter = 0
                    last_message_time = datetime.now(tz=timezone.utc)
                    
                    while True:
                        try:
                            # Add timeout to socket receive
                            msg = await asyncio.wait_for(stream.recv(), timeout=30.0)
                            
                            # Update last message time
                            current_time = datetime.now(tz=timezone.utc)
                            last_message_time = current_time
                            
                            if msg is None:
                                logger.warning("Received empty message")
                                continue
                            
                            # Process based on stream type
                            stream_type = msg.get('stream', '')
                            if 'depth' in stream_type:
                                await self._process_depth_message(msg['data'])
                            elif 'trade' in stream_type:
                                await self._process_trade_message(msg['data'])
                            
                            # Increment message counter
                            message_counter += 1
                            if message_counter % 100 == 0:
                                logger.info(f"Processed {message_counter} messages")
                            
                            # Periodic cleanup (every 1000 messages)
                            cleanup_counter += 1
                            if cleanup_counter >= 1000:
                                await self.cleanup_old_data()
                                cleanup_counter = 0
                            
                            # Update order filled data
                            await self._update_order_filled()
                            
                            # Save current state
                            await self._save_market_data(symbol)
                            
                            # Check for connection health
                            if (current_time - last_message_time).total_seconds() > 30:
                                logger.warning("No messages received for 30 seconds, reconnecting...")
                                break
                            
                        except asyncio.TimeoutError:
                            logger.warning("WebSocket message timeout, reconnecting...")
                            break
                        except Exception as e:
                            logger.error(f"Error processing message: {str(e)}")
                            if "Connection reset by peer" in str(e):
                                break
                            await asyncio.sleep(1)  # Brief pause before continuing
                            
            except Exception as e:
                logger.error(f"Stream error: {str(e)}")
            
            logger.info("Reconnecting to WebSocket stream...")
            await asyncio.sleep(5)  # Wait before reconnecting

    async def _get_order_book_snapshot(self, symbol: str) -> Dict:
        """Get initial order book snapshot"""
        try:
            depth = self.client.get_order_book(symbol=symbol, limit=1000)
            
            # Initialize order book with snapshot
            self.orderbook['bids'] = {float(price): float(qty) for price, qty in depth['bids']}
            self.orderbook['asks'] = {float(price): float(qty) for price, qty in depth['asks']}
            
            return depth
            
        except Exception as e:
            logger.error(f"Error getting order book snapshot: {str(e)}")
            raise

    async def _process_depth_message(self, msg: Dict):
        """Process order book update"""
        try:
            # Update bids
            for bid in msg.get('b', []):
                price, quantity = float(bid[0]), float(bid[1])
                if quantity > 0:
                    self.orderbook['bids'][price] = quantity
                else:
                    self.orderbook['bids'].pop(price, None)
                    
            # Update asks
            for ask in msg.get('a', []):
                price, quantity = float(ask[0]), float(ask[1])
                if quantity > 0:
                    self.orderbook['asks'][price] = quantity
                else:
                    self.orderbook['asks'].pop(price, None)
                    
        except Exception as e:
            logger.error(f"Error processing depth message: {str(e)}")
            
    async def _process_trade_message(self, msg: Dict):
        """Process trade update"""
        try:
            price = float(msg['p'])
            quantity = float(msg['q'])
            is_buyer = msg['m']
            # Use the actual trade timestamp from Binance
            trade_time = int(msg['T'])
            
            trade_data = {
                'quantity': quantity,
                'timestamp': datetime.fromtimestamp(trade_time / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC'),
                'trade_id': msg['t']
            }
            
            # Add to appropriate list
            if is_buyer:
                self.trade_history['bids'][price].append(trade_data)
            else:
                self.trade_history['asks'][price].append(trade_data)
            
            # Update order filled data
            await self._update_order_filled()
            
            # Cleanup old data periodically
            self.message_counter += 1
            if self.message_counter % 100 == 0:
                await self.cleanup_old_data()
            
        except Exception as e:
            logger.error(f"Error processing trade message: {str(e)}")
            
    def _calculate_unfilled_orders(self):
        """Calculate unfilled orders summary with price levels and values"""
        unfilled_summary = {
            'bids': [],
            'asks': []
        }
        
        # Process unfilled orders for both sides
        for side in ['bids', 'asks']:
            total_value = 0
            total_quantity = 0
            
            # Sort prices (descending for bids, ascending for asks)
            prices = sorted(self.orderbook[side].keys(), 
                          key=float, 
                          reverse=(side == 'bids'))
            
            for price in prices:
                price_float = float(price)
                quantity = self.orderbook[side][price]
                value = price_float * quantity
                
                total_quantity += quantity
                total_value += value
                
                unfilled_summary[side].append({
                    'price': price_float,
                    'quantity': quantity,
                    'value': value,
                    'cumulative_quantity': total_quantity,
                    'cumulative_value': total_value
                })
        
        return unfilled_summary

    async def _save_market_data(self, symbol: str):
        """Save all market data to JSON file"""
        try:
            # Get latest timestamp from trades
            latest_timestamp = None
            for side in ['bids', 'asks']:
                for trades in self.trade_history[side].values():
                    for trade in trades:
                        trade_time = datetime.strptime(trade['timestamp'], "%Y-%m-%d %H:%M:%S UTC")
                        if latest_timestamp is None or trade_time > latest_timestamp:
                            latest_timestamp = trade_time

            if latest_timestamp is None:
                latest_timestamp = datetime.now(timezone.utc)

            # Calculate unfilled orders summary
            unfilled_summary = self._calculate_unfilled_orders()
            
            # Calculate market metrics
            market_metrics = {
                'bids': {
                    'total_value': sum(level['value'] for level in unfilled_summary['bids']),
                    'total_quantity': sum(level['quantity'] for level in unfilled_summary['bids']),
                    'price_levels': len(unfilled_summary['bids']),
                    'best_bid': unfilled_summary['bids'][0]['price'] if unfilled_summary['bids'] else None
                },
                'asks': {
                    'total_value': sum(level['value'] for level in unfilled_summary['asks']),
                    'total_quantity': sum(level['quantity'] for level in unfilled_summary['asks']),
                    'price_levels': len(unfilled_summary['asks']),
                    'best_ask': unfilled_summary['asks'][0]['price'] if unfilled_summary['asks'] else None
                }
            }

            # Prepare data for saving
            market_data = {
                'timestamp': latest_timestamp.strftime("%Y-%m-%d %H:%M:%S UTC"),
                'symbol': symbol.lower(),
                'unfilled_orders': unfilled_summary,
                'market_metrics': market_metrics,
                'trade_history': self.trade_history
            }

            # Save to file
            filename = f'data/market_data_{symbol.lower()}.json'
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            
            with open(filename, 'w') as f:
                json.dump(market_data, f, indent=2)
            logger.info(f"Saved market data to {filename}")

        except Exception as e:
            logger.error(f"Error saving market data: {str(e)}")

def run_analyzer(symbol: str = 'BTCUSDT'):
    """Run the market data analyzer"""
    async def main():
        analyzer = MarketDataAnalyzer()
        try:
            await analyzer.start_market_stream(symbol.lower())
        except KeyboardInterrupt:
            logger.info("Shutting down gracefully...")
        except Exception as e:
            logger.error(f"Error in market stream: {str(e)}")

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down gracefully...")
    except Exception as e:
        logger.error(f"Error running analyzer: {str(e)}")

if __name__ == "__main__":
    run_analyzer()
