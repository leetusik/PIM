from app.crud.stock import (
    calculate_moving_averages,
    calculate_rs_momentum,
    calculate_rs_rankings,
    get_stocks,
)
from app.db.session import SessionLocal


def run_comprehensive_analysis():
    """
    Run comprehensive analysis in the correct order:
    1. Calculate moving averages and basic indicators for all stocks
    2. Calculate RS momentum for all stocks
    3. Calculate RS rankings based on the momentum data
    """
    db = SessionLocal()

    try:
        stocks = get_stocks(db)
        total_stocks = len(stocks)

        # Step 1: Calculate moving averages and basic indicators
        print(f"Step 1: Calculating moving averages for {total_stocks} stocks...")
        for i, stock in enumerate(stocks, 1):
            if i % 100 == 0 or i == total_stocks:
                print(
                    f"Processing MA {i}/{total_stocks}: {stock.name} ({stock.ticker})"
                )
            calculate_moving_averages(db, stock.id)
        print("Step 1 completed: Moving averages calculated!")

        # Step 2: Calculate RS momentum for all stocks
        print(f"\nStep 2: Calculating RS momentum for {total_stocks} stocks...")
        for i, stock in enumerate(stocks, 1):
            if i % 100 == 0 or i == total_stocks:
                print(
                    f"Processing RS {i}/{total_stocks}: {stock.name} ({stock.ticker})"
                )
            calculate_rs_momentum(db, stock.id)
        print("Step 2 completed: RS momentum calculated!")

        # Step 3: Calculate RS rankings across all stocks
        print("\nStep 3: Calculating RS rankings...")
        calculate_rs_rankings(db)
        print("Step 3 completed: RS rankings calculated!")

        print("\nComprehensive analysis completed successfully!")

    except Exception as e:
        print(f"Error in comprehensive analysis: {e}")
        db.rollback()
    finally:
        db.close()


def run_quick_update():
    """
    Quick update that only calculates for the most recent date
    Useful for daily updates
    """
    db = SessionLocal()

    try:
        from app.models.stock import DailyPrice
        from sqlalchemy import func

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

        print(f"Updating {len(stock_ids)} stocks with latest data...")

        # Update moving averages for stocks with new data
        for i, stock_id in enumerate(stock_ids, 1):
            if i % 100 == 0 or i == len(stock_ids):
                print(f"Processing MA {i}/{len(stock_ids)} stocks...")
            calculate_moving_averages(db, stock_id)

        # Update RS momentum for stocks with new data
        for i, stock_id in enumerate(stock_ids, 1):
            if i % 100 == 0 or i == len(stock_ids):
                print(f"Processing RS {i}/{len(stock_ids)} stocks...")
            calculate_rs_momentum(db, stock_id)

        # Calculate RS rankings for the latest date
        print("Calculating RS rankings...")
        calculate_rs_rankings(db, latest_date)

        print("Quick update completed!")

    except Exception as e:
        print(f"Error in quick update: {e}")
        db.rollback()
    finally:
        db.close()


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "quick":
        run_quick_update()
    else:
        run_comprehensive_analysis()
