from typing import List, Dict, Any, Tuple
from blessed import Terminal
import sys
import pandas as pd
from src.utils.logger import setup_logger
logger = setup_logger('display_manager')

class DisplayManager:
    def __init__(self):
        logger.debug("Initializing DisplayManager")
        self.term = Terminal()
        
    def update_screen(self, 
                     symbol: str,
                     timeframe: str,
                     latest_price: float,
                     last_update: str,
                     bullish_fvgs: List[Dict[str, Any]],
                     bearish_fvgs: List[Dict[str, Any]]):
        logger.debug(f"Updating display for {symbol} {timeframe}")
        """Update the terminal display in place"""
        # Clear screen and move to home position
        sys.stdout.write(self.term.clear)
        
        # Header with price info
        self._write_header(symbol, timeframe, latest_price, last_update)
        
        # Process FVGs
        active_bullish, active_bearish, mitigated_bullish, mitigated_bearish = self._categorize_fvgs(
            bullish_fvgs, bearish_fvgs
        )
        
        # Display Active FVGs
        self._display_active_fvgs(active_bullish, active_bearish)
        
        # Display Mitigated FVGs
        self._display_mitigated_fvgs(mitigated_bullish, mitigated_bearish)
        
        # Display Statistics
        self._display_statistics(
            bullish_fvgs, bearish_fvgs,
            active_bullish, active_bearish
        )
        
        # Footer
        self._write_footer()
        
        sys.stdout.flush()
    
    def _write_header(self, symbol: str, timeframe: str, latest_price: float, last_update: str):
        sys.stdout.write(self.term.white_on_blue(f" FVG Monitor - {symbol} {timeframe} ".center(self.term.width)) + "\n")
        sys.stdout.write(f"Price: {self.term.yellow(f'{latest_price:.2f}')} | Updated: {last_update}\n")
    
    def _categorize_fvgs(self, bullish_fvgs: List[Dict[str, Any]], bearish_fvgs: List[Dict[str, Any]]):
        """Categorize FVGs into active and mitigated"""
        def split_fvgs(fvgs: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
            active = []
            mitigated = []
            for fvg in fvgs:
                if (fvg.get('status', '').lower() == 'unfilled' and 
                    not any([fvg.get('first_mitigation_time'),
                            fvg.get('mitigation_time'),
                            fvg.get('time_to_mitigation')])):
                    active.append(fvg)
                else:
                    mitigated.append(fvg)
            return sorted(active, key=lambda x: x['time'], reverse=True), sorted(mitigated, key=lambda x: x['time'], reverse=True)
        
        active_bullish, mitigated_bullish = split_fvgs(bullish_fvgs)
        active_bearish, mitigated_bearish = split_fvgs(bearish_fvgs)
        
        return active_bullish, active_bearish, mitigated_bullish, mitigated_bearish
    
    def _display_active_fvgs(self, active_bullish: List[Dict[str, Any]], active_bearish: List[Dict[str, Any]]):
        sys.stdout.write(self.term.white_on_blue("\n Active FVGs ".ljust(self.term.width)) + "\n")
        sys.stdout.write(f"{'#':^4} {'Date':^12} {'Time (UTC)':^10} {'Type':^8} {'Gap Range':^30} {'Gap %':^10} {'Middle':^12}\n")
        sys.stdout.write("-" * self.term.width + "\n")
        
        # Sort by timestamp ascending (newest to oldest)
        all_active = sorted(active_bullish + active_bearish, key=lambda x: x['time'], reverse=True)
        
        if all_active:
            for idx, fvg in enumerate(all_active, 1):  # Start counting from 1
                is_bullish = fvg in active_bullish
                color = self.term.green if is_bullish else self.term.red
                direction = "▲" if is_bullish else "▼"
                date_str = fvg['time'].split()[0]
                time_str = fvg['time'].split()[1]
                
                sys.stdout.write(color(
                    f"{idx:^4} "  # Add numerical index
                    f"{date_str:^12} "
                    f"{time_str:^10} "
                    f"{direction:^8} "
                    f"{f'{fvg['gap_low']:.1f} - {fvg['gap_high']:.1f}':^30} "
                    f"{f'{fvg['gap_percentage']:.2f}%':^10} "
                    f"{fvg['middle_price']:.1f}".center(12) + "\n"
                ))
        else:
            sys.stdout.write("\nNo active FVGs\n")
    
    def _display_mitigated_fvgs(self, mitigated_bullish: List[Dict[str, Any]], mitigated_bearish: List[Dict[str, Any]]):
        sys.stdout.write(self.term.white_on_blue("\n Recent Mitigations ".ljust(self.term.width)) + "\n")
        
        # Column headers with adjusted widths
        headers = (
            f"{'#':^4} "
            f"{'Form Date':^12} "
            f"{'Form Time':^10} "
            f"{'Mit Date':^12} "
            f"{'Mit Time':^10} "
            f"{'Type':^6} "
            f"{'Gap Range':^20} "
            f"{'Hours':^8}"
        )
        sys.stdout.write(headers + "\n")
        sys.stdout.write("-" * self.term.width + "\n")
        
        all_mitigated = []
        for fvg in mitigated_bullish + mitigated_bearish:
            if fvg.get('first_mitigation_time'):
                all_mitigated.append(fvg)
        
        # Sort by mitigation time ascending (newest to oldest)
        all_mitigated.sort(key=lambda x: x['first_mitigation_time'], reverse=True)
        
        for idx, fvg in enumerate(all_mitigated, 1):  # Start counting from 1
            is_bullish = fvg in mitigated_bullish
            color = self.term.green if is_bullish else self.term.red
            direction = "▲" if is_bullish else "▼"
            form_date = fvg['time'].split()[0]
            form_time = fvg['time'].split()[1]
            mit_date = fvg['first_mitigation_time'].split()[0]
            mit_time = fvg['first_mitigation_time'].split()[1]
            time_to_mit = f"{fvg['time_to_mitigation']:.2f}" if fvg.get('time_to_mitigation') else "N/A"
            
            # Format each row with consistent spacing
            row = (
                f"{idx:^4} "
                f"{form_date:^12} "
                f"{form_time:^10} "
                f"{mit_date:^12} "
                f"{mit_time:^10} "
                f"{direction:^6} "
                f"{f'{fvg['gap_low']:.1f}-{fvg['gap_high']:.1f}':^20} "
                f"{time_to_mit:^8}"
            )
            sys.stdout.write(color(row + "\n"))
    
    def _display_statistics(self, bullish_fvgs: List[Dict[str, Any]], bearish_fvgs: List[Dict[str, Any]],
                          active_bullish: List[Dict[str, Any]], active_bearish: List[Dict[str, Any]]):
        sys.stdout.write(self.term.white_on_blue("\n Statistics ".ljust(self.term.width)) + "\n")
        total_fvgs = len(bullish_fvgs) + len(bearish_fvgs)
        stats_line = (
            f"Total FVGs: {total_fvgs} | "
            f"Bullish: {len(bullish_fvgs)} ({len(active_bullish)} active) | "
            f"Bearish: {len(bearish_fvgs)} ({len(active_bearish)} active)"
        )
        sys.stdout.write(stats_line + "\n\n")
    
    def _write_footer(self):
        sys.stdout.write(self.term.white_on_blue(" Press Ctrl+C to exit ".center(self.term.width))) 