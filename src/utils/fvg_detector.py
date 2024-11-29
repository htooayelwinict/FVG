from typing import List, Dict, Any, Tuple
import pandas as pd
from datetime import datetime

class FVGDetector:
    def __init__(self, candles: List[Dict[str, Any]]):
        """
        Initialize FVG detector with candlestick data
        :param candles: List of candlestick data dictionaries with Time, Open, High, Low, Close
        """
        self.candles = candles

    def detect_fvgs_only(self) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Detect both bullish and bearish Fair Value Gaps (FVGs) without mitigation check
        Returns two lists of FVGs with their properties
        """
        if len(self.candles) < 3:
            return [], []

        bullish_fvgs = []
        bearish_fvgs = []

        # Iterate through candles in reverse (newest to oldest)
        for i in range(len(self.candles) - 2):
            try:
                fvg = self._check_fvg_pattern(i)
                if fvg:
                    if fvg['type'] == 'bullish':
                        bullish_fvgs.append(fvg)
                    else:
                        bearish_fvgs.append(fvg)
            except IndexError and KeyError and TypeError as e:
                print(f"Error checking FVG pattern: {str(e)}")
                # Safety check in case we hit the end of the list
                break

        return bullish_fvgs, bearish_fvgs

    def _check_fvg_pattern(self, start_idx: int) -> Dict[str, Any]:
        """
        Check for FVG pattern at given index
        Returns FVG properties if found, None otherwise
        """
        third_candle = self.candles[start_idx]
        middle_candle = self.candles[start_idx + 1]
        first_candle = self.candles[start_idx + 2]

        # Check for bearish FVG (3rd candle's high < 1st candle's low)
        if third_candle['High'] < first_candle['Low']:
            gap_size = first_candle['Low'] - third_candle['High']
            gap_pct = (gap_size / third_candle['High']) * 100
            
            return {
                'type': 'bearish',
                'time': middle_candle['Time'],
                'gap_high': first_candle['Low'],
                'gap_low': third_candle['High'],
                'gap_size': gap_size,
                'gap_percentage': gap_pct,
                'middle_price': (first_candle['Low'] + third_candle['High']) / 2,
                'status': 'unfilled',
                'mitigation_time': None,
                'mitigation_price': None,
                'time_to_mitigation': None,
                'index': start_idx + 1
            }

        # Check for bullish FVG (3rd candle's low > 1st candle's high)
        if third_candle['Low'] > first_candle['High']:
            gap_size = third_candle['Low'] - first_candle['High']
            gap_pct = (gap_size / first_candle['High']) * 100
            
            return {
                'type': 'bullish',
                'time': middle_candle['Time'],
                'gap_low': first_candle['High'],
                'gap_high': third_candle['Low'],
                'gap_size': gap_size,
                'gap_percentage': gap_pct,
                'middle_price': (third_candle['Low'] + first_candle['High']) / 2,
                'status': 'unfilled',
                'mitigation_time': None,
                'mitigation_price': None,
                'time_to_mitigation': None,
                'index': start_idx + 1
            }
        
        return None

    def get_fvg_statistics(self, bullish_fvgs: List[Dict[str, Any]], bearish_fvgs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Calculate statistics for detected FVGs including mitigation statistics
        """
        total_fvgs = len(bullish_fvgs) + len(bearish_fvgs)
        
        # Calculate middle prices
        bullish_middles = [(fvg['gap_high'] + fvg['gap_low']) / 2 for fvg in bullish_fvgs]
        bearish_middles = [(fvg['gap_high'] + fvg['gap_low']) / 2 for fvg in bearish_fvgs]
        
        avg_bullish_middle = sum(bullish_middles) / len(bullish_fvgs) if bullish_fvgs else 0
        avg_bearish_middle = sum(bearish_middles) / len(bearish_fvgs) if bearish_fvgs else 0
        
        # Basic statistics
        avg_bullish_gap = sum(fvg['gap_size'] for fvg in bullish_fvgs) / len(bullish_fvgs) if bullish_fvgs else 0
        avg_bearish_gap = sum(fvg['gap_size'] for fvg in bearish_fvgs) / len(bearish_fvgs) if bearish_fvgs else 0
        avg_bullish_pct = sum(fvg['gap_percentage'] for fvg in bullish_fvgs) / len(bullish_fvgs) if bullish_fvgs else 0
        avg_bearish_pct = sum(fvg['gap_percentage'] for fvg in bearish_fvgs) / len(bearish_fvgs) if bearish_fvgs else 0

        # Mitigation statistics
        mitigated_bullish = sum(1 for fvg in bullish_fvgs if fvg['status'] == 'mitigated')
        mitigated_bearish = sum(1 for fvg in bearish_fvgs if fvg['status'] == 'mitigated')
        
        # Average time to mitigation (in hours)
        avg_bullish_time = sum(fvg['time_to_mitigation'] for fvg in bullish_fvgs if fvg['time_to_mitigation']) / mitigated_bullish if mitigated_bullish else 0
        avg_bearish_time = sum(fvg['time_to_mitigation'] for fvg in bearish_fvgs if fvg['time_to_mitigation']) / mitigated_bearish if mitigated_bearish else 0

        return {
            'total_fvgs': total_fvgs,
            'bullish_fvgs': len(bullish_fvgs),
            'bearish_fvgs': len(bearish_fvgs),
            'avg_bullish_gap': avg_bullish_gap,
            'avg_bearish_gap': avg_bearish_gap,
            'avg_bullish_gap_percentage': avg_bullish_pct,
            'avg_bearish_gap_percentage': avg_bearish_pct,
            'avg_bullish_middle': avg_bullish_middle,
            'avg_bearish_middle': avg_bearish_middle,
            'total_candles': len(self.candles),
            # Mitigation statistics
            'mitigated_bullish': mitigated_bullish,
            'mitigated_bearish': mitigated_bearish,
            'bullish_mitigation_rate': (mitigated_bullish / len(bullish_fvgs) * 100) if bullish_fvgs else 0,
            'bearish_mitigation_rate': (mitigated_bearish / len(bearish_fvgs) * 100) if bearish_fvgs else 0,
            'avg_bullish_mitigation_time': avg_bullish_time,
            'avg_bearish_mitigation_time': avg_bearish_time
        }