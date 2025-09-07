"""
Comprehensive analysis service - now uses consolidated stock_analysis
"""
from app.services.stock_analysis import run_comprehensive_analysis


def run_quick_update():
    """
    Quick update that only calculates for the most recent date
    Note: This still uses the old sequential approach - consider updating to batch processing
    """
    from app.db.session import SessionLocal
    from app.models.stock import DailyPrice
    from app.services.stock_analysis import (
        calculate_moving_averages_batch_parallel,
        calculate_rs_momentum_batch_parallel,
        calculate_rs_rankings
    )
    from sqlalchemy import func
    
    db = SessionLocal()
    
    try:
        # Get the most recent date
        latest_date = db.query(func.max(DailyPrice.date)).scalar()
        if not latest_date:
            print("No daily price data found")
            return
        
        print(f"Running quick update for {latest_date}...")
        
        # Get stocks that have data for the latest date
        stocks_with_latest_data = (
            db.query(DailyPrice.stock_id)
            .filter(DailyPrice.date == latest_date)
            .distinct()
            .all()
        )
        
        stock_ids = [row[0] for row in stocks_with_latest_data]
        print(f"Found {len(stock_ids)} stocks with latest data...")
        
        # For quick updates, we could filter to only process stocks with new data
        # But for now, let's run the full optimized analysis
        print("Running optimized batch analysis...")
        
        # Step 1: Moving averages
        print("Step 1: Calculating moving averages...")
        ma_results = calculate_moving_averages_batch_parallel(max_workers=4)
        
        # Step 2: RS momentum
        print("Step 2: Calculating RS momentum...")
        rs_results = calculate_rs_momentum_batch_parallel(max_workers=4)
        
        # Step 3: RS rankings for the latest date
        print("Step 3: Calculating RS rankings...")
        ranking_results = calculate_rs_rankings(latest_date)
        
        print("Quick update completed!")
        
        return {
            'moving_averages': ma_results,
            'rs_momentum': rs_results,
            'rs_rankings': ranking_results
        }
        
    except Exception as e:
        print(f"Error in quick update: {e}")
        return {'error': str(e)}
    finally:
        db.close()


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "quick":
        run_quick_update()
    else:
        run_comprehensive_analysis()
