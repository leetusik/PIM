"""
Clean CRUD operations for stocks - only basic database operations
Business logic should be in services layer
"""
from typing import List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import and_, func

from app.models.stock import DailyPrice, Stock
from app.schemas.stock import DailyPriceCreate, StockCreate


# ============================================================================
# BASIC CRUD OPERATIONS
# ============================================================================

def get_stocks(db: Session) -> List[Stock]:
    """Get all stocks"""
    return db.query(Stock).all()


def get_stock_by_id(db: Session, stock_id: int) -> Optional[Stock]:
    """Get stock by ID"""
    return db.query(Stock).filter(Stock.id == stock_id).first()


def get_stock_by_ticker(db: Session, ticker: str) -> Optional[Stock]:
    """Get stock by ticker"""
    return db.query(Stock).filter(Stock.ticker == ticker).first()


def create_stock(db: Session, stock: StockCreate) -> Stock:
    """Create a new stock"""
    db_stock = Stock(
        name=stock.name,
        market=stock.market,
        ticker=stock.ticker,
    )
    db.add(db_stock)
    db.commit()
    db.refresh(db_stock)
    return db_stock


def create_daily_price(db: Session, daily_price: DailyPriceCreate) -> DailyPrice:
    """Create a single daily price record"""
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


def bulk_create_daily_prices(db: Session, daily_prices_data: List[dict]) -> None:
    """Bulk insert daily prices - used by services"""
    from sqlalchemy.dialects.postgresql import insert
    
    if daily_prices_data:
        stmt = insert(DailyPrice).values(daily_prices_data)
        stmt = stmt.on_conflict_do_nothing(index_elements=['stock_id', 'date'])
        db.execute(stmt)
        db.commit()


def bulk_update_daily_prices(db: Session, updates_data: List[dict]) -> None:
    """Bulk update daily prices - used by services"""
    if updates_data:
        db.bulk_update_mappings(DailyPrice, updates_data)
        db.commit()


def get_daily_prices_for_stock(db: Session, stock_id: int, limit: Optional[int] = None) -> List[DailyPrice]:
    """Get daily prices for a specific stock, ordered by date"""
    query = db.query(DailyPrice).filter(DailyPrice.stock_id == stock_id).order_by(DailyPrice.date)
    if limit:
        query = query.limit(limit)
    return query.all()


def get_latest_daily_prices(db: Session, stock_ids: Optional[List[int]] = None) -> List[DailyPrice]:
    """Get latest daily prices for stocks"""
    # Subquery to get latest date for each stock
    latest_dates = db.query(
        DailyPrice.stock_id,
        func.max(DailyPrice.date).label('latest_date')
    ).group_by(DailyPrice.stock_id).subquery()
    
    # Main query
    query = db.query(DailyPrice).join(
        latest_dates,
        and_(
            DailyPrice.stock_id == latest_dates.c.stock_id,
            DailyPrice.date == latest_dates.c.latest_date
        )
    )
    
    if stock_ids:
        query = query.filter(DailyPrice.stock_id.in_(stock_ids))
    
    return query.all()


# ============================================================================
# QUERY HELPERS (for services layer)
# ============================================================================

def query_stocks_with_ma_filter(
    db: Session,
    min_price: Optional[float] = None,
    ma_50_filter: bool = True,
    ma_150_filter: bool = True,
    ma_200_filter: bool = True,
    limit: int = 100,
    offset: int = 0,
) -> List[Stock]:
    """Database query helper for moving average filtering"""
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


def query_stocks_with_trend_template_filter(
    db: Session,
    target_date: str = None,
    min_price: float = 20.0,
    min_rs_grade: float = 70.0,
    limit: int = 100,
) -> List[Stock]:
    """
    Database query helper for trend template filtering.
    This is a pure database operation - business logic should be in services layer.
    """
    if target_date is None:
        # Use the most recent trading date
        target_date = db.query(func.max(DailyPrice.date)).scalar()

    # STAGE 1: Cheap filters first (price and moving averages)
    basic_filter_query = (
        db.query(Stock)
        .join(DailyPrice, Stock.id == DailyPrice.stock_id)
        .filter(
            DailyPrice.date == target_date,
            DailyPrice.close >= min_price,  # Minimum price filter
            DailyPrice.close > DailyPrice.ma_50,  # Above 50-day MA
            DailyPrice.is_ma_200_bullish == True,  # MA 200 trending up
            DailyPrice.is_near_52w_high == True,  # Near 52-week high
            DailyPrice.ma_50 > DailyPrice.ma_150,  # MA 50 > MA 150
            DailyPrice.ma_150 > DailyPrice.ma_200,  # MA 150 > MA 200
        )
    )

    basic_filtered_stocks = basic_filter_query.all()

    if not basic_filtered_stocks:
        return []

    # STAGE 2: Apply RS filter on the pre-filtered stocks
    stock_ids = [stock.id for stock in basic_filtered_stocks]
    
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

    return final_query.all()
