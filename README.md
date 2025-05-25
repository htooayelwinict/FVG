# Fair Value Gap (FVG) Trading Analysis Tool

A cryptocurrency trading analysis tool focused on detecting and tracking Fair Value Gaps (FVGs) in price action. This tool connects to the Binance API to fetch real-time and historical market data, analyzes candlestick patterns to identify bullish and bearish FVGs, and tracks when these gaps are mitigated by subsequent price action.

## Features

- **Real-time FVG Detection**: Identifies bullish and bearish Fair Value Gaps as they form
- **Mitigation Tracking**: Monitors when FVGs are filled or mitigated by price action
- **Multiple Timeframes**: Supports various timeframes (M1, M3, M5, M15, M30, H1, H4, D1)
- **Terminal-based UI**: Color-coded display of active and mitigated FVGs
- **Statistics**: Provides metrics on gap sizes, mitigation rates, and timing
- **Order Book Analysis**: Analyzes market liquidity and order flow

## Getting Started

### Prerequisites

- Python 3.8+
- Binance API key and secret
- Required Python packages (see requirements.txt)

### Installation

1. Clone this repository
2. Create a virtual environment:
   ```
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```
3. Install dependencies:
   ```
   pip install -r requirements.txt
   ```
4. Create a `.env` file in the project root with your Binance API credentials:
   ```
   BINANCE_API_KEY=your_api_key
   BINANCE_API_SECRET=your_api_secret
   ```

### Usage

Run the main application:

```
python -m src.main
```

You will be prompted to select a timeframe and trading pair. The application will then start monitoring for FVGs in real-time.

## Core Components

1. **Main Application** (`src/main.py`): Entry point with FVGMonitor class running multiple threads for detection, price updates, and display.

2. **Detection Engine** (`src/backtest/detect_engine.py`): Core component for detecting FVGs in candlestick data.

3. **Data Loading** (`src/utils/data_loader.py`): BinanceDataLoader class for fetching market data from Binance API.

4. **FVG Detection** (`src/utils/fvg_detector.py`): Implements algorithms to detect bullish and bearish FVGs.

5. **Mitigation Detection** (`src/utils/mitigation_detector.py`): Tracks when FVGs are filled or mitigated by subsequent price action.

6. **Visualization** (`src/utils/display_manager.py`): Terminal-based UI for displaying FVGs and market data.

7. **Liquidity Analysis** (`src/utils/liquidity.py`): MarketDataAnalyzer class for order book and trade analysis.

## Understanding Fair Value Gaps (FVGs)

Fair Value Gaps are imbalances in price action that occur when price moves rapidly in one direction, creating a gap between candles. These gaps often represent areas where price may return to in the future, making them valuable for trading decisions.

- **Bullish FVG**: Forms when the low of the 3rd candle is higher than the high of the 1st candle
- **Bearish FVG**: Forms when the high of the 3rd candle is lower than the low of the 1st candle

The application detects these patterns and tracks when price returns to fill these gaps (mitigation).

## Acknowledgements

A special thanks to KweeBoss, the mentor and leader of the 1BullBear family, for guidance and inspiration in developing this trading analysis tool.

## License

This project is licensed under the terms of the license included in the repository.