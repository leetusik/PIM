from app.crud.stock import calculate_rs_momentum, get_stocks
from app.db.session import SessionLocal


def populate_all_rs_momentum():
    """Calculate and populate RS momentum for all stocks"""
    db = SessionLocal()

    try:
        stocks = get_stocks(db)
        total_stocks = len(stocks)

        print(f"Calculating RS momentum for {total_stocks} stocks...")

        for i, stock in enumerate(stocks, 1):
            print(f"Processing {i}/{total_stocks}: {stock.name} ({stock.ticker})")
            calculate_rs_momentum(db, stock.id)

        print("RS momentum calculation completed!")

    except Exception as e:
        print(f"Error calculating RS momentum: {e}")
        db.rollback()
    finally:
        db.close()


if __name__ == "__main__":
    populate_all_rs_momentum()
