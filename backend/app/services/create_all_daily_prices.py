import pandas as pd
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from typing import List, Optional
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import and_

from app.crud.stock import get_stocks
from app.db.session import SessionLocal
from app.models.stock import DailyPrice, Stock
from pykrx import stock

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


def process_single_stock(stock_obj: Stock, start_date: str = "20240101", end_date: str = "20250831") -> dict:
    """
    Process a single stock's daily prices in a separate thread with its own DB session.
    
    Args:
        stock_obj: Stock object to process
        start_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format
        
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
        'records_inserted': 0,
        'error': None,
        'processing_time': 0
    }
    
    start_time = time.time()
    
    try:
        # Get existing dates to avoid duplicates
        existing_dates = set(
            date[0] for date in db.query(DailyPrice.date)
            .filter(DailyPrice.stock_id == stock_obj.id)
            .all()
        )
        
        # Fetch OHLCV data from pykrx
        logger.info(f"Fetching data for {stock_obj.ticker} ({stock_obj.name})")
        ohlcv = stock.get_market_ohlcv(start_date, end_date, stock_obj.ticker)
        
        if ohlcv.empty:
            logger.warning(f"No data found for {stock_obj.ticker}")
            result['error'] = 'No data available'
            return result
        
        # Prepare batch data
        ohlcv["date"] = pd.to_datetime(ohlcv.index).normalize()
        daily_prices_data = []
        
        for index, row in ohlcv.iterrows():
            date_obj = row["date"].date()
            
            # Skip if we already have this date
            if date_obj in existing_dates:
                continue
                
            daily_prices_data.append({
                'stock_id': stock_obj.id,
                'date': date_obj,
                'open': float(row["시가"]) if pd.notna(row["시가"]) else None,
                'high': float(row["고가"]) if pd.notna(row["고가"]) else None,
                'low': float(row["저가"]) if pd.notna(row["저가"]) else None,
                'close': float(row["종가"]) if pd.notna(row["종가"]) else None,
                'volume': float(row["거래량"]) if pd.notna(row["거래량"]) else None,
            })
        
        result['records_processed'] = len(ohlcv)
        result['records_inserted'] = len(daily_prices_data)
        
        # Bulk insert if we have new data
        if daily_prices_data:
            # Use PostgreSQL's ON CONFLICT for upsert
            stmt = insert(DailyPrice).values(daily_prices_data)
            stmt = stmt.on_conflict_do_nothing(
                index_elements=['stock_id', 'date']
            )
            
            db.execute(stmt)
            db.commit()
            
            logger.info(f"✓ {stock_obj.ticker}: Inserted {len(daily_prices_data)} new records")
        else:
            logger.info(f"✓ {stock_obj.ticker}: No new records to insert")
        
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


def create_daily_prices_batch_parallel(
    max_workers: int = 4,
    start_date: str = "20240101", 
    end_date: str = "20250831",
    chunk_size: Optional[int] = None
) -> dict:
    """
    Create daily prices using batch processing with parallel execution.
    
    Args:
        max_workers: Maximum number of parallel threads
        start_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format
        chunk_size: Process stocks in chunks (None = process all at once)
        
    Returns:
        dict: Summary of processing results
    """
    global completed_stocks, total_stocks
    
    overall_start_time = time.time()
    logger.info("Starting batch parallel daily prices creation...")
    
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
        'total_records_processed': 0,
        'total_records_inserted': 0,
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
                executor.submit(process_single_stock, stock_obj, start_date, end_date): stock_obj
                for stock_obj in stock_chunk
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_stock):
                result = future.result()
                results['stock_results'].append(result)
                
                if result['success']:
                    results['successful'] += 1
                    results['total_records_processed'] += result['records_processed']
                    results['total_records_inserted'] += result['records_inserted']
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
    logger.info("PROCESSING COMPLETE - SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total stocks processed: {results['total_stocks']}")
    logger.info(f"Successful: {results['successful']}")
    logger.info(f"Failed: {results['failed']}")
    logger.info(f"Total records processed: {results['total_records_processed']:,}")
    logger.info(f"Total records inserted: {results['total_records_inserted']:,}")
    logger.info(f"Total processing time: {results['total_processing_time']:.2f} seconds")
    logger.info(f"Average time per stock: {results['average_time_per_stock']:.2f} seconds")
    
    if results['errors']:
        logger.warning(f"Errors encountered for {len(results['errors'])} stocks:")
        for error in results['errors']:
            logger.warning(f"  - {error['ticker']}: {error['error']}")
    
    return results


if __name__ == "__main__":
    # Configuration
    MAX_WORKERS = 4  # Adjust based on your system and database connection limits
    CHUNK_SIZE = None  # Process all stocks at once, or set to e.g., 50 for chunked processing
    
    # Run the optimized batch parallel processing
    results = create_daily_prices_batch_parallel(
        max_workers=MAX_WORKERS,
        chunk_size=CHUNK_SIZE
    )
