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


def calculate_rs_momentum(db: Session, stock_id: int):
    """Calculate RS momentum (ROC and RS score) for a specific stock"""
    # Get all daily prices for the stock ordered by date
    daily_prices = (
        db.query(DailyPrice)
        .filter(DailyPrice.stock_id == stock_id)
        .order_by(DailyPrice.date)
        .all()
    )

    if len(daily_prices) < 252:  # Need at least 252 days for 1-year ROC
        return

    # Convert to pandas for easier calculation
    df = pd.DataFrame(
        [
            {
                "id": dp.id,
                "date": dp.date,
                "close": dp.close,
            }
            for dp in daily_prices
        ]
    )

    # Calculate Rate of Change (ROC) for different periods
    df["roc_252"] = ((df["close"] / df["close"].shift(252)) - 1) * 100  # 1 year
    df["roc_126"] = ((df["close"] / df["close"].shift(126)) - 1) * 100  # 6 months
    df["roc_63"] = ((df["close"] / df["close"].shift(63)) - 1) * 100  # 3 months
    df["roc_21"] = ((df["close"] / df["close"].shift(21)) - 1) * 100  # 1 month

    # Calculate RS Momentum using IBD-style weighted formula
    df["rs_momentum"] = (
        df["roc_252"] * 0.4
        + df["roc_126"] * 0.2
        + df["roc_63"] * 0.2
        + df["roc_21"] * 0.2
    )

    # Update database records
    for _, row in df.iterrows():
        update_data = {}

        # Update RS fields
        if pd.notna(row["roc_252"]):
            update_data["roc_252"] = row["roc_252"]
        if pd.notna(row["roc_126"]):
            update_data["roc_126"] = row["roc_126"]
        if pd.notna(row["roc_63"]):
            update_data["roc_63"] = row["roc_63"]
        if pd.notna(row["roc_21"]):
            update_data["roc_21"] = row["roc_21"]
        if pd.notna(row["rs_momentum"]):
            update_data["rs_momentum"] = row["rs_momentum"]

        # Only update if we have data to update
        if update_data:
            db.query(DailyPrice).filter(DailyPrice.id == row["id"]).update(update_data)

    db.commit()


def calculate_rs_rankings(db: Session, target_date: str = None):
    """
    Calculate RS rankings for all stocks on a specific date.
    This is the expensive operation that should be run after basic filters.
    """
    from datetime import datetime

    if target_date is None:
        latest_date = db.query(func.max(DailyPrice.date)).scalar()
        target_date = latest_date

    stocks_with_momentum = (
        db.query(DailyPrice)
        .filter(DailyPrice.date == target_date, DailyPrice.rs_momentum.isnot(None))
        .all()
    )

    if not stocks_with_momentum:
        print(f"No RS momentum data found for date {target_date}")
        return

    momentum_data = [
        {"id": dp.id, "stock_id": dp.stock_id, "rs_momentum": dp.rs_momentum}
        for dp in stocks_with_momentum
    ]

    df = pd.DataFrame(momentum_data)

    if df.empty:
        print(f"DataFrame is empty for date {target_date}")
        return

    # Sort by rs_momentum descending and assign ranks
    df = df.sort_values("rs_momentum", ascending=False).reset_index(drop=True)
    df["rs_rank"] = range(1, len(df) + 1)

    # Calculate percentile grade (0-100 scale)
    total_stocks = len(df)
    df["rs_grade"] = ((total_stocks - df["rs_rank"]) / total_stocks) * 100

    # Prepare data for bulk update (ensuring native Python types)
    update_mappings = df[["id", "rs_rank", "rs_grade"]].to_dict("records")

    # Execute a single bulk update operation
    if update_mappings:
        db.bulk_update_mappings(DailyPrice, update_mappings)
        db.commit()
        print(f"Updated RS rankings for {len(df)} stocks on {target_date}")
    else:
        print("No records to update.")


def get_stocks_with_trend_template_filter(
    db: Session,
    target_date: str = None,
    min_price: float = 20.0,
    min_rs_grade: float = 70.0,
    limit: int = 100,
) -> List[Stock]:
    """
    Efficient funnel-based filtering for trend template stocks.
    Applies cheap filters first, then expensive RS calculations only on remaining stocks.
    """
    from datetime import datetime

    if target_date is None:
        # Use the most recent trading date
        target_date = db.query(func.max(DailyPrice.date)).scalar()

    print(f"Applying trend template filter for date: {target_date}")

    # STAGE 1: Cheap filters first (price and moving averages)
    # This should eliminate majority of stocks quickly
    basic_filter_query = (
        db.query(Stock)
        .join(DailyPrice, Stock.id == DailyPrice.stock_id)
        .filter(
            DailyPrice.date == target_date,
            DailyPrice.close >= min_price,  # Minimum price filter
            DailyPrice.close > DailyPrice.ma_50,  # Above 50-day MA
            # DailyPrice.close > DailyPrice.ma_150,  # Above 150-day MA
            # DailyPrice.close > DailyPrice.ma_200,  # Above 200-day MA
            DailyPrice.is_ma_200_bullish == True,  # MA 200 trending up
            DailyPrice.is_near_52w_high == True,  # Near 52-week high
            DailyPrice.ma_50 > DailyPrice.ma_150,  # MA 50 > MA 150
            DailyPrice.ma_150 > DailyPrice.ma_200,  # MA 150 > MA 200
        )
    )

    basic_filtered_stocks = basic_filter_query.all()
    print(f"After basic filters: {len(basic_filtered_stocks)} stocks remaining")

    if not basic_filtered_stocks:
        return []

    # STAGE 2: Calculate RS rankings only for stocks that passed basic filters
    stock_ids = [stock.id for stock in basic_filtered_stocks]

    # Check if RS rankings are already calculated for this date
    rs_data_exists = (
        db.query(DailyPrice)
        .filter(
            DailyPrice.date == target_date,
            DailyPrice.stock_id.in_(stock_ids),
            DailyPrice.rs_grade.isnot(None),
        )
        .first()
    )

    if not rs_data_exists:
        print("RS rankings not found, calculating...")
        calculate_rs_rankings(db, target_date)

    # STAGE 3: Apply RS filter on the pre-filtered stocks
    final_query = (
        db.query(Stock)
        .join(DailyPrice, Stock.id == DailyPrice.stock_id)
        .filter(
            DailyPrice.date == target_date,
            DailyPrice.stock_id.in_(stock_ids),  # Only check pre-filtered stocks
            DailyPrice.rs_grade >= min_rs_grade,  # RS grade >= 70
        )
        .order_by(DailyPrice.rs_grade.desc())
        .limit(limit)
    )

    final_stocks = final_query.all()
    print(f"After RS filter (>= {min_rs_grade}): {len(final_stocks)} stocks remaining")

    return final_stocks


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
