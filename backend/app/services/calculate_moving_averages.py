from app.crud.stock import get_stocks, calculate_moving_averages
from app.db.session import SessionLocal

def populate_all_moving_averages():
    """Calculate and populate moving averages for all stocks"""
    db = SessionLocal()
    
    try:
        stocks = get_stocks(db)
        total_stocks = len(stocks)
        
        print(f"Calculating moving averages for {total_stocks} stocks...")
        
        for i, stock in enumerate(stocks, 1):
            print(f"Processing {i}/{total_stocks}: {stock.name} ({stock.ticker})")
            calculate_moving_averages(db, stock.id)
            
        print("Moving averages calculation completed!")
        
    except Exception as e:
        print(f"Error calculating moving averages: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    populate_all_moving_averages()