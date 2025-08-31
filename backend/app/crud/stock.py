from typing import List, Optional

import pandas as pd
from app.models.stock import DailyPrice, Stock
from app.schemas.stock import DailyPriceCreate, StockCreate
from sqlalchemy import and_, func
from sqlalchemy.orm import Session


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
    """Calculate and update moving averages and extended analysis fields for a specific stock"""
    # Get all daily prices for the stock ordered by date
    daily_prices = (
        db.query(DailyPrice)
        .filter(DailyPrice.stock_id == stock_id)
        .order_by(DailyPrice.date)
        .all()
    )

    if (
        len(daily_prices) < 252
    ):  # Need at least 252 days (1 year) for 52-week calculations
        return

    # Convert to pandas for easier calculation
    df = pd.DataFrame(
        [
            {
                "id": dp.id,
                "date": dp.date,
                "close": dp.close,
                "high": dp.high,
                "low": dp.low,
            }
            for dp in daily_prices
        ]
    )

    # Calculate moving averages
    df["ma_50"] = df["close"].rolling(window=50, min_periods=50).mean()
    df["ma_150"] = df["close"].rolling(window=150, min_periods=150).mean()
    df["ma_200"] = df["close"].rolling(window=200, min_periods=200).mean()

    # Calculate MA 200 from 20 days ago
    df["ma_200_20d_ago"] = df["ma_200"].shift(20)

    # Calculate 52-week high and low (rolling 252 trading days)
    df["week_52_high"] = df["high"].rolling(window=252, min_periods=252).max()
    df["week_52_low"] = df["low"].rolling(window=252, min_periods=252).min()

    # Calculate boolean indicators
    df["is_ma_200_bullish"] = df["ma_200"] > df["ma_200_20d_ago"]
    df["is_near_52w_high"] = df["close"] >= (0.75 * df["week_52_high"])
    df["is_above_52w_low"] = df["close"] >= (1.25 * df["week_52_low"])

    # Update database records
    for _, row in df.iterrows():
        update_data = {}

        # Only update if we have valid MA data
        if pd.notna(row["ma_50"]):
            update_data["ma_50"] = row["ma_50"]
        if pd.notna(row["ma_150"]):
            update_data["ma_150"] = row["ma_150"]
        if pd.notna(row["ma_200"]):
            update_data["ma_200"] = row["ma_200"]

        # Update extended analysis fields
        if pd.notna(row["ma_200_20d_ago"]):
            update_data["ma_200_20d_ago"] = row["ma_200_20d_ago"]
            update_data["is_ma_200_bullish"] = bool(row["is_ma_200_bullish"])

        if pd.notna(row["week_52_high"]):
            update_data["week_52_high"] = row["week_52_high"]
            update_data["is_near_52w_high"] = bool(row["is_near_52w_high"])

        if pd.notna(row["week_52_low"]):
            update_data["week_52_low"] = row["week_52_low"]
            update_data["is_above_52w_low"] = bool(row["is_above_52w_low"])

        # Only update if we have data to update
        if update_data:
            db.query(DailyPrice).filter(DailyPrice.id == row["id"]).update(update_data)

    db.commit()


def calculate_extended_analysis(db: Session, stock_id: int):
    """Calculate extended analysis fields for a specific stock"""
    return calculate_moving_averages(db, stock_id)


def get_stocks_with_ma_filter(
    db: Session,
    min_price: Optional[float] = None,
    ma_50_filter: bool = True,
    ma_150_filter: bool = True,
    ma_200_filter: bool = True,
    limit: int = 100,
    offset: int = 0,
) -> List[Stock]:
    """Get stocks filtered by moving average criteria"""
    # Subquery to get latest daily price for each stock
    latest_prices = (
        db.query(DailyPrice.stock_id, func.max(DailyPrice.date).label("latest_date"))
        .group_by(DailyPrice.stock_id)
        .subquery()
    )

    # Main query joining stocks with their latest daily prices
    query = (
        db.query(Stock)
        .join(DailyPrice, Stock.id == DailyPrice.stock_id)
        .join(
            latest_prices,
            and_(
                DailyPrice.stock_id == latest_prices.c.stock_id,
                DailyPrice.date == latest_prices.c.latest_date,
            ),
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
