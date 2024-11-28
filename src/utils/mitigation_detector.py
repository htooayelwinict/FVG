from typing import List, Dict, Any
import pandas as pd

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
        self.candles = candles
    
    def _get_timeframe_delta(self, time1: str, time2: str) -> pd.Timedelta:
        """
        Calculate the timeframe delta between two candles.
        This helps determine the appropriate waiting time for FVG formation.
        """
        t1 = pd.to_datetime(time1)
        t2 = pd.to_datetime(time2)
        delta = abs(t2 - t1)
        return delta
    
    def check_mitigations(self, bullish_fvgs: List[Dict[str, Any]], bearish_fvgs: List[Dict[str, Any]]):
        """
        Check if each FVG has been mitigated by subsequent price action.
        Records both the first and last candles that enter the FVG zone.
        """
        if len(self.candles) < 2:
            return
            
        # Calculate timeframe by looking at first two candles
        timeframe_delta = self._get_timeframe_delta(self.candles[0]['Time'], self.candles[1]['Time'])
        
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
            
            fvg_time = pd.to_datetime(fvg['time'])
            formation_complete_time = fvg_time + timeframe_delta
            
            candles_after = sorted(
                [c for c in self.candles if pd.to_datetime(c['Time']) > formation_complete_time],
                key=lambda x: pd.to_datetime(x['Time'])
            )
            
            in_gap_zone = False
            first_mitigation_found = False
            
            for current_candle in candles_after:
                current_time = pd.to_datetime(current_candle['Time'])
                
                # Check if price enters gap zone
                price_in_gap = (
                    current_candle['Low'] <= fvg['gap_high'] if is_bullish
                    else current_candle['High'] >= fvg['gap_low']
                )
                
                if price_in_gap:
                    if not first_mitigation_found:
                        fvg['status'] = 'mitigated'
                        fvg['first_mitigation_time'] = current_candle['Time']
                        time_diff = current_time - formation_complete_time
                        fvg['time_to_mitigation'] = time_diff.total_seconds() / 3600
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
