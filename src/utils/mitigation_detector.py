from typing import List, Dict, Any
import pandas as pd
import datetime

class MitigationDetector:
    """
    Detects mitigation of Fair Value Gaps (FVGs) in price action.
    Works in conjunction with FVGDetector but kept separate for modularity.
    """
    
    def __init__(self, candles: List[Dict[str, Any]]):
        """
        Initialize the mitigation detector.
        
        Args:
            candles: List of candlestick data, each candle containing Time, Open, High, Low, Close
        """
        # Reverse the candles to get chronological order (oldest first)
        self.candles = list(reversed(candles))
        # Pre-convert all timestamps once
        for candle in self.candles:
            candle['timestamp'] = datetime.datetime.strptime(candle['Time'], '%Y-%m-%d %H:%M:%S UTC').timestamp()
    
    def _get_timeframe_seconds(self) -> float:
        """Calculate timeframe in seconds from first two candles"""
        if len(self.candles) < 2:
            return 0
        return abs(self.candles[0]['timestamp'] - self.candles[1]['timestamp'])
    
    def check_mitigations(self, bullish_fvgs: List[Dict[str, Any]], bearish_fvgs: List[Dict[str, Any]]):
        """
        Check if each FVG has been mitigated by subsequent price action.
        Records both the first and last candles that enter the FVG zone.
        """
        if len(self.candles) < 2:
            return
            
        timeframe_seconds = self._get_timeframe_seconds()
        
        def check_fvg_mitigation(fvg: Dict[str, Any], is_bullish: bool):
            """Helper function to check mitigation for a single FVG"""
            if 'status' not in fvg:
                fvg.update({
                    'status': 'unfilled',
                    'first_mitigation_time': None,
                    'last_mitigation_time': None,
                    'time_to_mitigation': None
                })
            
            if fvg['status'] == 'mitigated':
                return
            
            fvg_timestamp = datetime.datetime.strptime(fvg['time'], '%Y-%m-%d %H:%M:%S UTC').timestamp()
            formation_complete_time = fvg_timestamp + timeframe_seconds
            
            # Filter and sort candles after formation (already in chronological order)
            candles_after = [c for c in self.candles if c['timestamp'] > formation_complete_time]
            
            in_gap_zone = False
            first_mitigation_found = False
            
            for current_candle in candles_after:
                # Check if price enters gap zone
                price_in_gap = (
                    current_candle['Low'] <= fvg['gap_high'] if is_bullish
                    else current_candle['High'] >= fvg['gap_low']
                )
                
                if price_in_gap:
                    if not first_mitigation_found:
                        fvg['status'] = 'mitigated'
                        fvg['first_mitigation_time'] = current_candle['Time']
                        time_diff = current_candle['timestamp'] - formation_complete_time
                        fvg['time_to_mitigation'] = time_diff / 3600  # Convert to hours
                        first_mitigation_found = True
                    in_gap_zone = True
                    fvg['last_mitigation_time'] = current_candle['Time']
                else:
                    if in_gap_zone:
                        break
        
        # Check mitigations for both types of FVGs
        for fvg in bullish_fvgs:
            check_fvg_mitigation(fvg, is_bullish=True)
        
        for fvg in bearish_fvgs:
            check_fvg_mitigation(fvg, is_bullish=False)
