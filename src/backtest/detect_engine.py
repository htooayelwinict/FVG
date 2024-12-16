from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
from src.utils.fvg_detector import FVGDetector
from src.utils.data_loader import BinanceDataLoader
from src.utils.mitigation_detector import MitigationDetector
from src.utils.logger import setup_logger

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

    def detect_fvgs(self) -> Dict[str, Any]:
        """Detect FVGs from last 200 candles"""
        # Get symbol from data source
        symbol = self.data_source.split('://')[1].split('_')[0]
        
        # Load candles
        candles = self.data_loader.load_candles(symbol=symbol, timeframe=self.timeframe)
        if not candles:
            logger.warning(f"No candles found for {symbol} {self.timeframe}")
            return self._create_empty_results()

        # Initialize detectors with candles
        fvg_detector = FVGDetector(candles)
        
        # Detect FVGs
        bullish_fvgs, bearish_fvgs = fvg_detector.detect_fvgs_only()
        
        # Check for mitigations
        mitigation_detector = MitigationDetector(candles)
        mitigation_detector.check_mitigations(bullish_fvgs, bearish_fvgs)

        results = {
            'bullish_fvgs': bullish_fvgs,
            'bearish_fvgs': bearish_fvgs,
            'last_update': datetime.utcnow().isoformat()
        }

        return results

    def _create_empty_results(self) -> Dict[str, Any]:
        """Create empty results structure"""
        return {
            'bullish_fvgs': [],
            'bearish_fvgs': [],
            'last_update': datetime.utcnow().isoformat()
        }