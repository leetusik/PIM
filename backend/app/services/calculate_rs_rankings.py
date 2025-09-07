"""
RS rankings and trend template filtering - now uses consolidated stock_analysis
"""
from app.services.stock_analysis import (
    calculate_rs_rankings,
    get_stocks_with_trend_template_filter
)


def calculate_daily_rs_rankings(target_date: str = None):
    """Calculate RS rankings for all stocks for a specific date"""
    print("Starting RS rankings calculation...")
    result = calculate_rs_rankings(target_date)
    
    if result['success']:
        print("RS rankings calculation completed!")
    else:
        print(f"Error calculating RS rankings: {result['error']}")
    
    return result


def find_trend_template_stocks(target_date: str = None, min_rs_grade: float = 70.0):
    """Find stocks that match the trend template criteria using efficient funnel filtering"""
    print("Starting trend template stock filtering...")
    
    # Use the consolidated service for filtering
    trend_stocks = get_stocks_with_trend_template_filter(
        target_date=target_date,
        min_price=20.0,  # Minimum price filter
        min_rs_grade=min_rs_grade,  # RS grade >= 70
        limit=100,
    )
    
    print(f"\nFound {len(trend_stocks)} stocks matching trend template:")
    
    # Display top 10 results
    if trend_stocks:
        from app.db.session import SessionLocal
        from app.models.stock import DailyPrice
        
        db = SessionLocal()
        try:
            for stock in trend_stocks[:10]:
                latest_price = (
                    db.query(DailyPrice)
                    .filter(
                        DailyPrice.stock_id == stock.id,
                        DailyPrice.date == (
                            target_date or 
                            db.query(DailyPrice.date).order_by(DailyPrice.date.desc()).first()[0]
                        ),
                    )
                    .first()
                )
                
                if latest_price:
                    print(
                        f"  {stock.name} ({stock.ticker}): "
                        f"Price: {latest_price.close:.0f}, "
                        f"RS Grade: {latest_price.rs_grade:.1f}"
                    )
        finally:
            db.close()
    
    return trend_stocks


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "rankings":
        # Calculate RS rankings only
        target_date = sys.argv[2] if len(sys.argv) > 2 else None
        calculate_daily_rs_rankings(target_date)
    elif len(sys.argv) > 1 and sys.argv[1] == "filter":
        # Find trend template stocks
        target_date = sys.argv[2] if len(sys.argv) > 2 else None
        min_rs = float(sys.argv[3]) if len(sys.argv) > 3 else 70.0
        find_trend_template_stocks(target_date, min_rs)
    else:
        # Default: calculate rankings then find trend stocks
        calculate_daily_rs_rankings()
        find_trend_template_stocks()
