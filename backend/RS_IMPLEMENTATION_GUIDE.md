# Relative Strength (RS) Implementation Guide

## Overview

The RS implementation follows the IBD-style methodology for identifying market-leading stocks with strong relative performance. The system uses a weighted Rate of Change (ROC) formula and efficient funnel-based filtering.

## RS Formula

```
RS_Momentum = (ROC(252일) × 0.4) + (ROC(126일) × 0.2) + (ROC(63일) × 0.2) + (ROC(21일) × 0.2)
```

Where ROC = ((Current Price / Price N days ago) - 1) × 100

## Database Fields Added

### DailyPrice Model
- `roc_252`: Rate of Change 252 days (1 year)
- `roc_126`: Rate of Change 126 days (6 months) 
- `roc_63`: Rate of Change 63 days (3 months)
- `roc_21`: Rate of Change 21 days (1 month)
- `rs_momentum`: Combined RS momentum score
- `rs_rank`: Rank among all stocks (1 = best)
- `rs_grade`: Percentile score (0-100, 100 = best)

## Functions

### 1. `calculate_moving_averages(db, stock_id)`
- Calculates MA 50, 150, 200 and basic extended analysis fields
- Handles 52-week high/low calculations and MA trend indicators
- **Separated from RS calculations for modularity**

### 2. `calculate_rs_momentum(db, stock_id)`
- **NEW: Separate function for RS calculations**
- Calculates ROC for 252, 126, 63, 21 day periods
- Computes RS momentum using IBD-style weighted formula
- Updates `roc_*` and `rs_momentum` fields

### 3. `calculate_rs_rankings(db, target_date)`
- Calculates RS rankings across ALL stocks for a specific date
- Expensive operation - should be run after basic filtering
- Updates `rs_rank` and `rs_grade` fields

### 4. `get_stocks_with_trend_template_filter(db, target_date, min_price, min_rs_grade, limit)`
- **Efficient funnel-based filtering system**
- Stage 1: Cheap filters (price, moving averages) - eliminates majority quickly
- Stage 2: Calculate RS rankings only for remaining stocks
- Stage 3: Apply RS filter (>= 70 grade) on pre-filtered stocks

## Services

### 1. `calculate_moving_averages.py`
```bash
# Calculate moving averages and basic indicators only
docker-compose run --rm backend python app/services/calculate_moving_averages.py
```

### 2. `calculate_rs_momentum.py` (NEW)
```bash
# Calculate RS momentum (ROC and RS scores) for all stocks
docker-compose run --rm backend python app/services/calculate_rs_momentum.py
```

### 3. `calculate_rs_rankings.py`
```bash
# Calculate RS rankings only
docker-compose run --rm backend python app/services/calculate_rs_rankings.py rankings

# Find trend template stocks
docker-compose run --rm backend python app/services/calculate_rs_rankings.py filter

# Both operations
docker-compose run --rm backend python app/services/calculate_rs_rankings.py
```

### 4. `comprehensive_analysis.py` (Updated)
```bash
# Full analysis (all stocks, all calculations in sequence)
docker-compose run --rm backend python app/services/comprehensive_analysis.py

# Quick daily update (latest date only)
docker-compose run --rm backend python app/services/comprehensive_analysis.py quick
```

## Trend Template Criteria

The efficient filter applies these criteria in order:

### Stage 1: Basic Filters (Cheap Operations)
- Price >= 20 KRW
- Close > MA 50
- Close > MA 150  
- Close > MA 200
- MA 200 trending up (bullish)
- Near 52-week high (>= 75% of 52w high)
- MA 50 > MA 150
- MA 150 > MA 200

### Stage 2: RS Calculation (Expensive)
- Calculate RS rankings only for stocks passing Stage 1

### Stage 3: RS Filter
- RS Grade >= 70 (top 30% of all stocks)

## Usage Workflow

### Full Initial Setup
1. **Calculate moving averages**:
   ```bash
   docker-compose run --rm backend python app/services/calculate_moving_averages.py
   ```

2. **Calculate RS momentum**:
   ```bash
   docker-compose run --rm backend python app/services/calculate_rs_momentum.py
   ```

3. **Calculate RS rankings**:
   ```bash
   docker-compose run --rm backend python app/services/calculate_rs_rankings.py rankings
   ```

4. **Find trend template stocks**:
   ```bash
   docker-compose run --rm backend python app/services/calculate_rs_rankings.py filter
   ```

### Automated Workflow (Recommended)
```bash
# Run all steps in sequence automatically
docker-compose run --rm backend python app/services/comprehensive_analysis.py
```

### Quick Daily Update
```bash
# For daily updates with new price data (runs MA + RS + Rankings)
docker-compose run --rm backend python app/services/comprehensive_analysis.py quick
```

## Performance Optimization

The funnel-based filtering system is designed for efficiency:

1. **Stage 1** eliminates ~90% of stocks with cheap operations
2. **Stage 2** only calculates expensive RS rankings for remaining ~10% 
3. **Stage 3** applies final RS filter

This approach can handle 2,000+ stocks efficiently by avoiding expensive calculations on unsuitable stocks.

## Example Output

```
Applying trend template filter for date: 2024-01-15
After basic filters: 156 stocks remaining
RS rankings not found, calculating...
Updated RS rankings for 156 stocks on 2024-01-15
After RS filter (>= 70.0): 23 stocks remaining

Found 23 stocks matching trend template:
  삼성전자 (005930): Price: 68900, RS Grade: 89.2
  SK하이닉스 (000660): Price: 142500, RS Grade: 87.5
  ...
```

## Migration

The database migration has been applied automatically. All new fields are nullable and indexed for performance.
