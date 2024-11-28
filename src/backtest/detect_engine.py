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
    def __init__(self, data_source: str, timeframe: str, start_date: str, batch_size: int = 100, data_loader=None, cache_duration: int = None):
        """Initialize the Detection Engine"""
        logger.debug(f"Initializing DetectionEngine with {data_source}, {timeframe}")
        self.data_source = data_source
        self.timeframe = timeframe
        self.start_date = start_date
        
        # Apply optimized batch size
        self.batch_size = 300  # Reduced from 300 to 200 for faster processing
        
        self.data_loader = data_loader if data_loader else BinanceDataLoader()
        self.bullish_fvgs = []
        self.bearish_fvgs = []
        self.all_candles = []
        self.total_candles_analyzed = 0
        
        # Add new attributes for optimization with faster cache
        self.cache = {
            'candles': {},
            'fvgs': {},
            'hashes': {},  # Store hashes for validation
            'last_update': None,
            'cache_duration': cache_duration if cache_duration is not None else 60  # 1 minute default
        }
        
        # Add processing queues
        self.candle_queue = Queue()
        
        # Processing flags
        self.is_processing = False
        self.processing_lock = threading.Lock()
        
    def _load_batch(self, current_date: str, last_date: Optional[str]) -> Tuple[List[Dict[str, Any]], str, bool]:
        """Load a batch of candles with smaller overlap"""
        symbol = self.data_source.split('://')[1].split('_')[0]
        
        # Reduced overlap size
        overlap_size = 5  # Reduced from 10 to 5
        request_size = self.batch_size + overlap_size
        
        candles, last_date, has_more = self.data_loader.load_candlestick_batch(
            symbol=symbol,
            timeframe=self.timeframe,
            start_date=last_date if last_date else current_date,
            batch_size=request_size
        )
        
        return candles, last_date, has_more

    def _generate_cache_hash(self, data: Any) -> str:
        """Generate a hash for cache validation"""
        if isinstance(data, (list, dict)):
            # Convert to JSON string for consistent hashing
            data_str = json.dumps(data, sort_keys=True)
        else:
            data_str = str(data)
        
        return hashlib.md5(data_str.encode()).hexdigest()

    def _update_cache(self, key: str, data: Any) -> None:
        """Update cache with hash validation"""
        self.cache['last_update'] = datetime.now()
        self.cache[key] = data
        self.cache['hashes'][key] = self._generate_cache_hash(data)
        logger.debug(f"Cache updated for {key} with hash {self.cache['hashes'][key]}")

    def _is_cache_valid(self) -> bool:
        """Check if cache is still valid with hash verification"""
        if not self.cache['last_update']:
            return False
        
        elapsed = (datetime.now() - self.cache['last_update']).total_seconds()
        if elapsed >= self.cache['cache_duration']:
            return False
            
        # Verify cache integrity using hashes
        for key in ['fvgs', 'candles']:
            if key in self.cache and key in self.cache['hashes']:
                current_hash = self._generate_cache_hash(self.cache[key])
                if current_hash != self.cache['hashes'][key]:
                    logger.warning(f"Cache hash mismatch for {key}")
                    return False
        
        return True

    def get_cached_results(self) -> Optional[Dict[str, Any]]:
        """Get cached results with hash validation"""
        if not self._is_cache_valid():
            return None
            
        if 'fvgs' in self.cache:
            # Verify FVG cache hash
            current_hash = self._generate_cache_hash(self.cache['fvgs'])
            if current_hash != self.cache['hashes'].get('fvgs'):
                logger.warning("FVG cache hash mismatch, invalidating cache")
                return None
                
            logger.debug("Using valid cached FVG results")
            return self.cache['fvgs']
            
        return None

    def collect_all_fvgs(self, target_fvgs: int = 20) -> Dict[str, Any]:
        """Collect FVGs with hash-validated caching"""
        # Check cache first with hash validation
        cached_results = self.get_cached_results()
        if cached_results:
            return cached_results
            
        # Reset state
        self.bullish_fvgs = []
        self.bearish_fvgs = []
        self.all_candles = []
        self.total_candles_analyzed = 0
        
        current_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        has_more_data = True
        last_date = None
        temp_bullish = []
        temp_bearish = []
        
        # Initialize detectors once
        fvg_detector = None
        mitigation_detector = None
        
        # Single batch for initial detection
        batch_candles, last_date, has_more_data = self._load_batch(current_date, last_date)
        if not batch_candles:
            return self._create_empty_results()
        
        self.all_candles.extend(batch_candles)
        
        # Initialize detectors with all candles
        fvg_detector = FVGDetector(self.all_candles)
        mitigation_detector = MitigationDetector(self.all_candles)
        
        # Detect FVGs in one pass
        bullish_batch, bearish_batch = fvg_detector.detect_fvgs_only()
        
        # Add unique FVGs
        temp_bullish.extend(fvg for fvg in bullish_batch if self._is_unique_fvg(fvg, temp_bullish))
        temp_bearish.extend(fvg for fvg in bearish_batch if self._is_unique_fvg(fvg, temp_bearish))
        
        # Sort by time
        temp_bullish.sort(key=lambda x: x['time'], reverse=True)
        temp_bearish.sort(key=lambda x: x['time'], reverse=True)
        
        # Take only what we need
        self.bullish_fvgs = temp_bullish[:target_fvgs]
        self.bearish_fvgs = temp_bearish[:target_fvgs]
        
        # Check mitigations only for the FVGs we'll use
        mitigation_detector.check_mitigations(self.bullish_fvgs, self.bearish_fvgs)
        
        # Cache results with hash
        results = {
            'bullish_fvgs': self.bullish_fvgs,
            'bearish_fvgs': self.bearish_fvgs,
            'total_candles': len(self.all_candles),
            'timeframe': self.timeframe,
            'analysis_period': f"{self.start_date} to current",
            'collection_complete': True
        }
        self._update_cache('fvgs', results)
        
        logger.debug(f"Found {len(temp_bullish)} bullish and {len(temp_bearish)} bearish FVGs")
        return results

    def _create_empty_results(self) -> Dict[str, Any]:
        """Create empty results structure"""
        return {
            'bullish_fvgs': [],
            'bearish_fvgs': [],
            'total_candles': 0,
            'timeframe': self.timeframe,
            'analysis_period': f"{self.start_date} to current",
            'collection_complete': False
        }

    def _is_unique_fvg(self, fvg: Dict[str, Any], fvg_list: List[Dict[str, Any]]) -> bool:
        """Enhanced uniqueness check for FVGs"""
        return not any(
            existing['time'] == fvg['time'] and 
            abs(existing['gap_size'] - fvg['gap_size']) < 0.0001 and
            existing['type'] == fvg['type']
            for existing in fvg_list
        )

    def analyze_fvgs(self, target_fvgs: int = 20) -> Dict[str, Any]:
        """Maintain compatibility with existing code"""
        return self.collect_all_fvgs(target_fvgs)

    def _preload_next_batch(self, current_date: str, last_date: str) -> None:
        """Preload next batch of data in background"""
        def _preload():
            try:
                next_batch, _, _ = self._load_batch(current_date, last_date)
                if next_batch:
                    self.candle_queue.put(next_batch)
            except Exception as e:
                print(f"Error preloading batch: {str(e)}")
        
        threading.Thread(target=_preload, daemon=True).start()

    def _merge_fvg_results(self, current: Dict[str, List], new: Dict[str, List]) -> Dict[str, List]:
        """Merge FVG results intelligently"""
        merged = {
            'bullish_fvgs': [],
            'bearish_fvgs': []
        }
        
        # Helper to deduplicate FVGs
        def _deduplicate(fvgs: List) -> List:
            seen = set()
            unique = []
            for fvg in fvgs:
                key = f"{fvg['time']}_{fvg['gap_size']}"
                if key not in seen:
                    seen.add(key)
                    unique.append(fvg)
            return unique
        
        # Merge and deduplicate
        for key in ['bullish_fvgs', 'bearish_fvgs']:
            combined = current.get(key, []) + new.get(key, [])
            merged[key] = _deduplicate(combined)
            
        return merged