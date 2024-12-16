from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
from src.utils.fvg_detector import FVGDetector
from src.utils.data_loader import BinanceDataLoader
from src.utils.mitigation_detector import MitigationDetector
from src.utils.logger import setup_logger
import threading
from queue import Queue
import time
import hashlib
import json

logger = setup_logger('detection_engine')

class DetectionEngine:
    def __init__(self, data_source: str, timeframe: str):
        """Initialize the Detection Engine"""
        logger.debug(f"Initializing DetectionEngine with {data_source}, {timeframe}")
        self.data_source = data_source
        self.timeframe = timeframe
        self.data_loader = BinanceDataLoader()
        self.bullish_fvgs = []
        self.bearish_fvgs = []
        self.all_candles = []
        
        # Cache for results
        self.cache = {
            'fvgs': {},
            'last_update': None,
            'cache_duration': 60  # 1 minute default
        }

    def detect_fvgs(self) -> Dict[str, Any]:
        """Detect FVGs from last 500 candles"""
        # Check cache first
        if self._is_cache_valid():
            logger.debug("Using cached FVG results")
            return self.cache['fvgs']
        
        # Get symbol from data source
        symbol = self.data_source.split('://')[1].split('_')[0]
        
        # Load 500 candles
        self.all_candles = self.data_loader.load_candles(
            symbol=symbol,
            timeframe=self.timeframe
        )
        
        if not self.all_candles:
            return self._create_empty_results()
        
        # Initialize detectors
        fvg_detector = FVGDetector(self.all_candles)
        mitigation_detector = MitigationDetector(self.all_candles)
        
        # Detect FVGs
        self.bullish_fvgs, self.bearish_fvgs = fvg_detector.detect_fvgs_only()
        
        # Check mitigations
        mitigation_detector.check_mitigations(self.bullish_fvgs, self.bearish_fvgs)
        
        # Prepare results
        results = {
            'bullish_fvgs': self.bullish_fvgs,
            'bearish_fvgs': self.bearish_fvgs,
            'total_candles': len(self.all_candles),
            'timeframe': self.timeframe,
            'collection_complete': True
        }
        
        # Update cache
        self._update_cache(results)
        
        logger.debug(f"Found {len(self.bullish_fvgs)} bullish and {len(self.bearish_fvgs)} bearish FVGs")
        return results

    def _is_cache_valid(self) -> bool:
        """Check if cache is still valid"""
        if not self.cache['last_update']:
            return False
        
        elapsed = (datetime.now() - self.cache['last_update']).total_seconds()
        return elapsed < self.cache['cache_duration']

    def _update_cache(self, results: Dict[str, Any]) -> None:
        """Update cache with new results"""
        self.cache['last_update'] = datetime.now()
        self.cache['fvgs'] = results

    def _create_empty_results(self) -> Dict[str, Any]:
        """Create empty results structure"""
        return {
            'bullish_fvgs': [],
            'bearish_fvgs': [],
            'total_candles': 0,
            'timeframe': self.timeframe,
            'collection_complete': False
        }