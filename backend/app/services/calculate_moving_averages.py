import pandas as pd
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from typing import List, Optional
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import update

from app.crud.stock import get_stocks
from app.db.session import SessionLocal
from app.models.stock import DailyPrice, Stock

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Thread-safe counter for progress tracking
progress_lock = Lock()
completed_stocks = 0
total_stocks = 0


def calculate_moving_averages_single_stock(stock_obj: Stock) -> dict:
    """
    Calculate moving averages for a single stock in a separate thread with its own DB session.
    
    Args:
        stock_obj: Stock object to process
        
    Returns:
        dict: Processing result with status and metrics
    """
    global completed_stocks
    
    # Create a new database session for this thread
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
        daily_prices = db.query(DailyPrice).filter(
            DailyPrice.stock_id == stock_obj.id
        ).order_by(DailyPrice.date).all()
        
        if len(daily_prices) < 200:
            logger.warning(f"Skipping {stock_obj.ticker}: Only {len(daily_prices)} records (need 200+ for MA200)")
            result['error'] = f'Insufficient data: {len(daily_prices)} records (need 200+)'
            return result
        
        logger.info(f"Calculating moving averages for {stock_obj.ticker} ({len(daily_prices)} records)")
        
        # Convert to pandas for easier calculation
        df = pd.DataFrame([{
            'id': dp.id,
            'date': dp.date,
            'close': dp.close
        } for dp in daily_prices])
        
        # Calculate moving averages
        df['ma_50'] = df['close'].rolling(window=50, min_periods=50).mean()
        df['ma_150'] = df['close'].rolling(window=150, min_periods=150).mean()
        df['ma_200'] = df['close'].rolling(window=200, min_periods=200).mean()
        
        # Prepare batch updates
        updates_to_process = []
        for _, row in df.iterrows():
            if pd.notna(row['ma_50']) and pd.notna(row['ma_150']) and pd.notna(row['ma_200']):
                updates_to_process.append({
                    'id': int(row['id']),
                    'ma_50': float(row['ma_50']),
                    'ma_150': float(row['ma_150']),
                    'ma_200': float(row['ma_200'])
                })
        
        result['records_processed'] = len(daily_prices)
        result['records_updated'] = len(updates_to_process)
        
        # Batch update using SQLAlchemy's bulk_update_mappings
        if updates_to_process:
            # Split into smaller chunks to avoid memory issues with very large datasets
            chunk_size = 1000
            total_updated = 0
            
            for i in range(0, len(updates_to_process), chunk_size):
                chunk = updates_to_process[i:i + chunk_size]
                
                # Use bulk update for better performance
                db.bulk_update_mappings(DailyPrice, chunk)
                total_updated += len(chunk)
            
            db.commit()
            logger.info(f"✓ {stock_obj.ticker}: Updated {total_updated} records with moving averages")
        else:
            logger.info(f"✓ {stock_obj.ticker}: No records to update (insufficient data for MAs)")
        
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
) -> dict:
    """
    Calculate moving averages using batch processing with parallel execution.
    
    Args:
        max_workers: Maximum number of parallel threads
        chunk_size: Process stocks in chunks (None = process all at once)
        
    Returns:
        dict: Summary of processing results
    """
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
        
        if chunk_size:
            logger.info(f"Processing in chunks of {chunk_size}")
            
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
            # Submit all tasks
            future_to_stock = {
                executor.submit(calculate_moving_averages_single_stock, stock_obj): stock_obj
                for stock_obj in stock_chunk
            }
            
            # Collect results as they complete
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
    
    if results['errors']:
        logger.warning(f"Errors encountered for {len(results['errors'])} stocks:")
        for error in results['errors']:
            logger.warning(f"  - {error['ticker']}: {error['error']}")
    
    return results


def populate_all_moving_averages():
    """Legacy function for backward compatibility"""
    logger.warning("Using legacy function. Consider using calculate_moving_averages_batch_parallel() for better performance.")
    return calculate_moving_averages_batch_parallel()


if __name__ == "__main__":
    # Configuration
    MAX_WORKERS = 4  # Adjust based on your system and database connection limits
    CHUNK_SIZE = None  # Process all stocks at once, or set to e.g., 50 for chunked processing
    
    # Run the optimized batch parallel processing
    results = calculate_moving_averages_batch_parallel(
        max_workers=MAX_WORKERS,
        chunk_size=CHUNK_SIZE
    )