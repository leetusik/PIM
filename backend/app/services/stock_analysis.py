"""
Consolidated stock analysis services with batch + parallel processing
All business logic for stock calculations should be here
"""
import pandas as pd
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from typing import List, Optional, Dict, Any
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import func

from app.crud.stock import (
    get_stocks, 
    get_daily_prices_for_stock,
    bulk_update_daily_prices,
    query_stocks_with_trend_template_filter,
    query_stocks_with_ma_filter
)
from app.db.session import SessionLocal
from app.models.stock import DailyPrice, Stock

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Thread-safe counters
progress_lock = Lock()
completed_stocks = 0
total_stocks = 0


# ============================================================================
# MOVING AVERAGES CALCULATION
# ============================================================================

def calculate_moving_averages_single_stock(stock_obj: Stock) -> Dict[str, Any]:
    """Calculate moving averages and extended analysis for a single stock"""
    global completed_stocks
    
    db = SessionLocal()
    result = {
        'stock_id': stock_obj.id,
        'ticker': stock_obj.ticker,
        'name': stock_obj.name,
        'success': False,
        'records_processed': 0,
        'records_updated': 0,
        'error': None,
        'processing_time': 0
    }
    
    start_time = time.time()
    
    try:
        # Get all daily prices for the stock ordered by date
        daily_prices = get_daily_prices_for_stock(db, stock_obj.id)
        
        if len(daily_prices) < 252:  # Need at least 252 days (1 year) for 52-week calculations
            logger.warning(f"Skipping {stock_obj.ticker}: Only {len(daily_prices)} records (need 252+ for full analysis)")
            result['error'] = f'Insufficient data: {len(daily_prices)} records (need 252+)'
            return result
        
        logger.info(f"Calculating moving averages for {stock_obj.ticker} ({len(daily_prices)} records)")
        
        # Convert to pandas for easier calculation
        df = pd.DataFrame([{
            'id': dp.id,
            'date': dp.date,
            'close': dp.close,
            'high': dp.high,
            'low': dp.low,
        } for dp in daily_prices])
        
        # Calculate moving averages
        df['ma_50'] = df['close'].rolling(window=50, min_periods=50).mean()
        df['ma_150'] = df['close'].rolling(window=150, min_periods=150).mean()
        df['ma_200'] = df['close'].rolling(window=200, min_periods=200).mean()
        
        # Calculate MA 200 from 20 days ago
        df['ma_200_20d_ago'] = df['ma_200'].shift(20)
        
        # Calculate 52-week high and low (rolling 252 trading days)
        df['week_52_high'] = df['high'].rolling(window=252, min_periods=252).max()
        df['week_52_low'] = df['low'].rolling(window=252, min_periods=252).min()
        
        # Calculate boolean indicators
        df['is_ma_200_bullish'] = df['ma_200'] > df['ma_200_20d_ago']
        df['is_near_52w_high'] = df['close'] >= (0.75 * df['week_52_high'])
        df['is_above_52w_low'] = df['close'] >= (1.25 * df['week_52_low'])
        
        # Prepare batch updates
        updates_to_process = []
        for _, row in df.iterrows():
            update_data = {'id': int(row['id'])}
            
            # Only update if we have valid MA data
            if pd.notna(row['ma_50']):
                update_data['ma_50'] = float(row['ma_50'])
            if pd.notna(row['ma_150']):
                update_data['ma_150'] = float(row['ma_150'])
            if pd.notna(row['ma_200']):
                update_data['ma_200'] = float(row['ma_200'])
            
            # Update extended analysis fields
            if pd.notna(row['ma_200_20d_ago']):
                update_data['ma_200_20d_ago'] = float(row['ma_200_20d_ago'])
                update_data['is_ma_200_bullish'] = bool(row['is_ma_200_bullish'])
            
            if pd.notna(row['week_52_high']):
                update_data['week_52_high'] = float(row['week_52_high'])
                update_data['is_near_52w_high'] = bool(row['is_near_52w_high'])
            
            if pd.notna(row['week_52_low']):
                update_data['week_52_low'] = float(row['week_52_low'])
                update_data['is_above_52w_low'] = bool(row['is_above_52w_low'])
            
            # Only add if we have data to update beyond just the ID
            if len(update_data) > 1:
                updates_to_process.append(update_data)
        
        result['records_processed'] = len(daily_prices)
        result['records_updated'] = len(updates_to_process)
        
        # Batch update
        if updates_to_process:
            # Split into chunks to avoid memory issues
            chunk_size = 1000
            for i in range(0, len(updates_to_process), chunk_size):
                chunk = updates_to_process[i:i + chunk_size]
                bulk_update_daily_prices(db, chunk)
            
            logger.info(f"✓ {stock_obj.ticker}: Updated {len(updates_to_process)} records with moving averages")
        else:
            logger.info(f"✓ {stock_obj.ticker}: No records to update")
        
        result['success'] = True
        
    except Exception as e:
        logger.error(f"✗ Error processing {stock_obj.ticker}: {str(e)}")
        result['error'] = str(e)
        db.rollback()
        
    finally:
        result['processing_time'] = time.time() - start_time
        db.close()
        
        # Update progress counter (thread-safe)
        with progress_lock:
            completed_stocks += 1
            logger.info(f"Progress: {completed_stocks}/{total_stocks} stocks completed")
    
    return result


def calculate_moving_averages_batch_parallel(
    max_workers: int = 4,
    chunk_size: Optional[int] = None
) -> Dict[str, Any]:
    """Calculate moving averages using batch processing with parallel execution"""
    global completed_stocks, total_stocks
    
    overall_start_time = time.time()
    logger.info("Starting batch parallel moving averages calculation...")
    
    # Get all stocks
    db = SessionLocal()
    try:
        stocks = get_stocks(db)
        total_stocks = len(stocks)
        completed_stocks = 0
        
        logger.info(f"Found {total_stocks} stocks to process")
        logger.info(f"Using {max_workers} parallel workers")
        
    finally:
        db.close()
    
    # Results tracking
    results = {
        'total_stocks': total_stocks,
        'successful': 0,
        'failed': 0,
        'skipped': 0,
        'total_records_processed': 0,
        'total_records_updated': 0,
        'total_processing_time': 0,
        'errors': [],
        'stock_results': []
    }
    
    # Process stocks in chunks if specified
    if chunk_size:
        stock_chunks = [stocks[i:i + chunk_size] for i in range(0, len(stocks), chunk_size)]
    else:
        stock_chunks = [stocks]
    
    for chunk_idx, stock_chunk in enumerate(stock_chunks, 1):
        if chunk_size:
            logger.info(f"Processing chunk {chunk_idx}/{len(stock_chunks)} ({len(stock_chunk)} stocks)")
        
        # Process chunk in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_stock = {
                executor.submit(calculate_moving_averages_single_stock, stock_obj): stock_obj
                for stock_obj in stock_chunk
            }
            
            for future in as_completed(future_to_stock):
                result = future.result()
                results['stock_results'].append(result)
                
                if result['success']:
                    results['successful'] += 1
                    results['total_records_processed'] += result['records_processed']
                    results['total_records_updated'] += result['records_updated']
                else:
                    if 'Insufficient data' in str(result['error']):
                        results['skipped'] += 1
                    else:
                        results['failed'] += 1
                        results['errors'].append({
                            'ticker': result['ticker'],
                            'error': result['error']
                        })
    
    # Calculate summary statistics
    results['total_processing_time'] = time.time() - overall_start_time
    results['average_time_per_stock'] = results['total_processing_time'] / total_stocks if total_stocks > 0 else 0
    
    # Log final summary
    logger.info("=" * 60)
    logger.info("MOVING AVERAGES CALCULATION COMPLETE - SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total stocks processed: {results['total_stocks']}")
    logger.info(f"Successful: {results['successful']}")
    logger.info(f"Skipped (insufficient data): {results['skipped']}")
    logger.info(f"Failed: {results['failed']}")
    logger.info(f"Total records processed: {results['total_records_processed']:,}")
    logger.info(f"Total records updated: {results['total_records_updated']:,}")
    logger.info(f"Total processing time: {results['total_processing_time']:.2f} seconds")
    logger.info(f"Average time per stock: {results['average_time_per_stock']:.2f} seconds")
    
    return results


# ============================================================================
# RS MOMENTUM CALCULATION
# ============================================================================

def calculate_rs_momentum_single_stock(stock_obj: Stock) -> Dict[str, Any]:
    """Calculate RS momentum (ROC and RS score) for a single stock"""
    global completed_stocks
    
    db = SessionLocal()
    result = {
        'stock_id': stock_obj.id,
        'ticker': stock_obj.ticker,
        'name': stock_obj.name,
        'success': False,
        'records_processed': 0,
        'records_updated': 0,
        'error': None,
        'processing_time': 0
    }
    
    start_time = time.time()
    
    try:
        # Get all daily prices for the stock ordered by date
        daily_prices = get_daily_prices_for_stock(db, stock_obj.id)
        
        if len(daily_prices) < 252:  # Need at least 252 days for 1-year ROC
            logger.warning(f"Skipping {stock_obj.ticker}: Only {len(daily_prices)} records (need 252+ for RS momentum)")
            result['error'] = f'Insufficient data: {len(daily_prices)} records (need 252+)'
            return result
        
        logger.info(f"Calculating RS momentum for {stock_obj.ticker} ({len(daily_prices)} records)")
        
        # Convert to pandas for easier calculation
        df = pd.DataFrame([{
            'id': dp.id,
            'date': dp.date,
            'close': dp.close,
        } for dp in daily_prices])
        
        # Calculate Rate of Change (ROC) for different periods
        df['roc_252'] = ((df['close'] / df['close'].shift(252)) - 1) * 100  # 1 year
        df['roc_126'] = ((df['close'] / df['close'].shift(126)) - 1) * 100  # 6 months
        df['roc_63'] = ((df['close'] / df['close'].shift(63)) - 1) * 100   # 3 months
        df['roc_21'] = ((df['close'] / df['close'].shift(21)) - 1) * 100   # 1 month
        
        # Calculate RS Momentum using IBD-style weighted formula
        df['rs_momentum'] = (
            df['roc_252'] * 0.4 +
            df['roc_126'] * 0.2 +
            df['roc_63'] * 0.2 +
            df['roc_21'] * 0.2
        )
        
        # Prepare batch updates
        updates_to_process = []
        for _, row in df.iterrows():
            update_data = {'id': int(row['id'])}
            
            # Update RS fields
            if pd.notna(row['roc_252']):
                update_data['roc_252'] = float(row['roc_252'])
            if pd.notna(row['roc_126']):
                update_data['roc_126'] = float(row['roc_126'])
            if pd.notna(row['roc_63']):
                update_data['roc_63'] = float(row['roc_63'])
            if pd.notna(row['roc_21']):
                update_data['roc_21'] = float(row['roc_21'])
            if pd.notna(row['rs_momentum']):
                update_data['rs_momentum'] = float(row['rs_momentum'])
            
            # Only add if we have data to update beyond just the ID
            if len(update_data) > 1:
                updates_to_process.append(update_data)
        
        result['records_processed'] = len(daily_prices)
        result['records_updated'] = len(updates_to_process)
        
        # Batch update
        if updates_to_process:
            chunk_size = 1000
            for i in range(0, len(updates_to_process), chunk_size):
                chunk = updates_to_process[i:i + chunk_size]
                bulk_update_daily_prices(db, chunk)
            
            logger.info(f"✓ {stock_obj.ticker}: Updated {len(updates_to_process)} records with RS momentum")
        else:
            logger.info(f"✓ {stock_obj.ticker}: No records to update")
        
        result['success'] = True
        
    except Exception as e:
        logger.error(f"✗ Error processing {stock_obj.ticker}: {str(e)}")
        result['error'] = str(e)
        db.rollback()
        
    finally:
        result['processing_time'] = time.time() - start_time
        db.close()
        
        with progress_lock:
            completed_stocks += 1
            logger.info(f"Progress: {completed_stocks}/{total_stocks} stocks completed")
    
    return result


def calculate_rs_momentum_batch_parallel(
    max_workers: int = 4,
    chunk_size: Optional[int] = None
) -> Dict[str, Any]:
    """Calculate RS momentum using batch processing with parallel execution"""
    global completed_stocks, total_stocks
    
    overall_start_time = time.time()
    logger.info("Starting batch parallel RS momentum calculation...")
    
    # Get all stocks
    db = SessionLocal()
    try:
        stocks = get_stocks(db)
        total_stocks = len(stocks)
        completed_stocks = 0
        
        logger.info(f"Found {total_stocks} stocks to process")
        logger.info(f"Using {max_workers} parallel workers")
        
    finally:
        db.close()
    
    # Results tracking
    results = {
        'total_stocks': total_stocks,
        'successful': 0,
        'failed': 0,
        'skipped': 0,
        'total_records_processed': 0,
        'total_records_updated': 0,
        'total_processing_time': 0,
        'errors': [],
        'stock_results': []
    }
    
    # Process stocks in chunks if specified
    if chunk_size:
        stock_chunks = [stocks[i:i + chunk_size] for i in range(0, len(stocks), chunk_size)]
    else:
        stock_chunks = [stocks]
    
    for chunk_idx, stock_chunk in enumerate(stock_chunks, 1):
        if chunk_size:
            logger.info(f"Processing chunk {chunk_idx}/{len(stock_chunks)} ({len(stock_chunk)} stocks)")
        
        # Process chunk in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_stock = {
                executor.submit(calculate_rs_momentum_single_stock, stock_obj): stock_obj
                for stock_obj in stock_chunk
            }
            
            for future in as_completed(future_to_stock):
                result = future.result()
                results['stock_results'].append(result)
                
                if result['success']:
                    results['successful'] += 1
                    results['total_records_processed'] += result['records_processed']
                    results['total_records_updated'] += result['records_updated']
                else:
                    if 'Insufficient data' in str(result['error']):
                        results['skipped'] += 1
                    else:
                        results['failed'] += 1
                        results['errors'].append({
                            'ticker': result['ticker'],
                            'error': result['error']
                        })
    
    # Calculate summary statistics
    results['total_processing_time'] = time.time() - overall_start_time
    results['average_time_per_stock'] = results['total_processing_time'] / total_stocks if total_stocks > 0 else 0
    
    # Log final summary
    logger.info("=" * 60)
    logger.info("RS MOMENTUM CALCULATION COMPLETE - SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total stocks processed: {results['total_stocks']}")
    logger.info(f"Successful: {results['successful']}")
    logger.info(f"Skipped (insufficient data): {results['skipped']}")
    logger.info(f"Failed: {results['failed']}")
    logger.info(f"Total records processed: {results['total_records_processed']:,}")
    logger.info(f"Total records updated: {results['total_records_updated']:,}")
    logger.info(f"Total processing time: {results['total_processing_time']:.2f} seconds")
    
    return results


# ============================================================================
# RS RANKINGS CALCULATION
# ============================================================================

def calculate_rs_rankings(target_date: str = None) -> Dict[str, Any]:
    """Calculate RS rankings for all stocks on a specific date"""
    db = SessionLocal()
    
    try:
        if target_date is None:
            latest_date = db.query(func.max(DailyPrice.date)).scalar()
            target_date = latest_date
        
        logger.info(f"Calculating RS rankings for date: {target_date}")
        
        stocks_with_momentum = (
            db.query(DailyPrice)
            .filter(DailyPrice.date == target_date, DailyPrice.rs_momentum.isnot(None))
            .all()
        )
        
        if not stocks_with_momentum:
            logger.warning(f"No RS momentum data found for date {target_date}")
            return {'success': False, 'error': 'No RS momentum data found'}
        
        momentum_data = [
            {"id": dp.id, "stock_id": dp.stock_id, "rs_momentum": dp.rs_momentum}
            for dp in stocks_with_momentum
        ]
        
        df = pd.DataFrame(momentum_data)
        
        if df.empty:
            logger.warning(f"DataFrame is empty for date {target_date}")
            return {'success': False, 'error': 'Empty DataFrame'}
        
        # Sort by rs_momentum descending and assign ranks
        df = df.sort_values("rs_momentum", ascending=False).reset_index(drop=True)
        df["rs_rank"] = range(1, len(df) + 1)
        
        # Calculate percentile grade (0-100 scale)
        total_stocks = len(df)
        df["rs_grade"] = ((total_stocks - df["rs_rank"]) / total_stocks) * 100
        
        # Prepare data for bulk update
        update_mappings = df[["id", "rs_rank", "rs_grade"]].to_dict("records")
        
        # Execute bulk update
        if update_mappings:
            bulk_update_daily_prices(db, update_mappings)
            logger.info(f"✓ Updated RS rankings for {len(df)} stocks on {target_date}")
            
            return {
                'success': True,
                'date': target_date,
                'stocks_updated': len(df),
                'processing_time': 0  # Add timing if needed
            }
        else:
            logger.warning("No records to update")
            return {'success': False, 'error': 'No records to update'}
    
    except Exception as e:
        logger.error(f"Error calculating RS rankings: {e}")
        db.rollback()
        return {'success': False, 'error': str(e)}
    finally:
        db.close()


# ============================================================================
# COMPREHENSIVE ANALYSIS
# ============================================================================

def run_comprehensive_analysis(max_workers: int = 4) -> Dict[str, Any]:
    """
    Run comprehensive analysis in the correct order:
    1. Calculate moving averages and basic indicators for all stocks
    2. Calculate RS momentum for all stocks  
    3. Calculate RS rankings based on the momentum data
    """
    overall_start = time.time()
    logger.info("Starting comprehensive stock analysis...")
    
    results = {
        'moving_averages': None,
        'rs_momentum': None,
        'rs_rankings': None,
        'total_time': 0,
        'success': False
    }
    
    try:
        # Step 1: Calculate moving averages
        logger.info("Step 1: Calculating moving averages...")
        results['moving_averages'] = calculate_moving_averages_batch_parallel(max_workers=max_workers)
        
        # Step 2: Calculate RS momentum
        logger.info("Step 2: Calculating RS momentum...")
        results['rs_momentum'] = calculate_rs_momentum_batch_parallel(max_workers=max_workers)
        
        # Step 3: Calculate RS rankings
        logger.info("Step 3: Calculating RS rankings...")
        results['rs_rankings'] = calculate_rs_rankings()
        
        results['total_time'] = time.time() - overall_start
        results['success'] = True
        
        logger.info("=" * 60)
        logger.info("COMPREHENSIVE ANALYSIS COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Total processing time: {results['total_time']:.2f} seconds")
        
    except Exception as e:
        logger.error(f"Error in comprehensive analysis: {e}")
        results['error'] = str(e)
    
    return results


# ============================================================================
# TREND TEMPLATE FILTERING
# ============================================================================

def get_stocks_with_trend_template_filter(
    target_date: str = None,
    min_price: float = 20.0,
    min_rs_grade: float = 70.0,
    limit: int = 100,
) -> List[Stock]:
    """Find stocks that match the trend template criteria using efficient funnel filtering"""
    db = SessionLocal()
    
    try:
        logger.info("Starting trend template stock filtering...")
        
        # Check if RS rankings exist for the target date
        if target_date is None:
            target_date = db.query(func.max(DailyPrice.date)).scalar()
        
        rs_data_exists = (
            db.query(DailyPrice)
            .filter(
                DailyPrice.date == target_date,
                DailyPrice.rs_grade.isnot(None),
            )
            .first()
        )
        
        if not rs_data_exists:
            logger.info("RS rankings not found, calculating...")
            calculate_rs_rankings(target_date)
        
        # Use the CRUD query helper for filtering
        trend_stocks = query_stocks_with_trend_template_filter(
            db=db,
            target_date=target_date,
            min_price=min_price,
            min_rs_grade=min_rs_grade,
            limit=limit,
        )
        
        logger.info(f"Found {len(trend_stocks)} stocks matching trend template criteria")
        return trend_stocks
        
    except Exception as e:
        logger.error(f"Error finding trend template stocks: {e}")
        return []
    finally:
        db.close()


# ============================================================================
# MOVING AVERAGE FILTERING
# ============================================================================

def get_stocks_with_ma_filter(
    min_price: Optional[float] = None,
    ma_50_filter: bool = True,
    ma_150_filter: bool = True,
    ma_200_filter: bool = True,
    limit: int = 100,
    offset: int = 0,
) -> List[Stock]:
    """
    Business service for filtering stocks by moving average criteria.
    Handles session management and logging.
    """
    db = SessionLocal()
    
    try:
        logger.info(f"Filtering stocks with MA criteria: min_price={min_price}, "
                   f"ma_50={ma_50_filter}, ma_150={ma_150_filter}, ma_200={ma_200_filter}")
        
        # Use the CRUD query helper for filtering
        stocks = query_stocks_with_ma_filter(
            db=db,
            min_price=min_price,
            ma_50_filter=ma_50_filter,
            ma_150_filter=ma_150_filter,
            ma_200_filter=ma_200_filter,
            limit=limit,
            offset=offset,
        )
        
        logger.info(f"Found {len(stocks)} stocks matching MA filter criteria")
        return stocks
        
    except Exception as e:
        logger.error(f"Error filtering stocks with MA criteria: {e}")
        return []
    finally:
        db.close()
