from app.models.stock import DailyPrice, Stock
from app.schemas.stock import DailyPriceCreate, StockCreate
from sqlalchemy.orm import Session
from sqlalchemy import func, and_
from typing import List, Optional
import pandas as pd


def get_stocks(db: Session):
    return db.query(Stock).all()


def create_stock(db: Session, stock: StockCreate):
    db_stock = Stock(
        name=stock.name,
        market=stock.market,
        ticker=stock.ticker,
        # dart_code=stock.dart_code,
    )
    db.add(db_stock)
    db.commit()
    db.refresh(db_stock)

    return db_stock


def create_daily_price(db: Session, daily_price: DailyPriceCreate):
    db_daily_price = DailyPrice(
        stock_id=daily_price.stock_id,
        date=daily_price.date,
        open=daily_price.open,
        high=daily_price.high,
        low=daily_price.low,
        close=daily_price.close,
        volume=daily_price.volume,
    )
    db.add(db_daily_price)
    db.commit()
    db.refresh(db_daily_price)

    return db_daily_price


def calculate_moving_averages(db: Session, stock_id: int):
    """Calculate and update moving averages for a specific stock"""
    # Get all daily prices for the stock ordered by date
    daily_prices = db.query(DailyPrice).filter(
        DailyPrice.stock_id == stock_id
    ).order_by(DailyPrice.date).all()
    
    if len(daily_prices) < 200:
        return  # Need at least 200 days for 200MA
    
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
    
    # Update database records
    for _, row in df.iterrows():
        if pd.notna(row['ma_50']) and pd.notna(row['ma_150']) and pd.notna(row['ma_200']):
            db.query(DailyPrice).filter(DailyPrice.id == row['id']).update({
                'ma_50': row['ma_50'],
                'ma_150': row['ma_150'],
                'ma_200': row['ma_200']
            })
    
    db.commit()


def get_stocks_with_ma_filter(
    db: Session,
    min_price: Optional[float] = None,
    ma_50_filter: bool = True,
    ma_150_filter: bool = True, 
    ma_200_filter: bool = True,
    limit: int = 100,
    offset: int = 0
) -> List[Stock]:
    """Get stocks filtered by moving average criteria"""
    # Subquery to get latest daily price for each stock
    latest_prices = db.query(
        DailyPrice.stock_id,
        func.max(DailyPrice.date).label('latest_date')
    ).group_by(DailyPrice.stock_id).subquery()
    
    # Main query joining stocks with their latest daily prices
    query = db.query(Stock).join(
        DailyPrice, Stock.id == DailyPrice.stock_id
    ).join(
        latest_prices,
        and_(
            DailyPrice.stock_id == latest_prices.c.stock_id,
            DailyPrice.date == latest_prices.c.latest_date
        )
    )
    
    # Apply filters
    filters = []
    
    if min_price:
        filters.append(DailyPrice.close >= min_price)
    
    if ma_50_filter:
        filters.append(DailyPrice.close > DailyPrice.ma_50)
    
    if ma_150_filter:
        filters.append(DailyPrice.close > DailyPrice.ma_150)
        
    if ma_200_filter:
        filters.append(DailyPrice.close > DailyPrice.ma_200)
    
    # Apply all filters
    if filters:
        query = query.filter(and_(*filters))
    
    return query.offset(offset).limit(limit).all()
