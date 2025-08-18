from typing import Optional

from app.api.v1 import deps
from app.crud.stock import get_stocks_with_ma_filter
from app.schemas.stock import (
    DailyPriceResponse,
    StockScreenResponse,
    StockWithLatestPrice,
)
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

router = APIRouter()


@router.get("/screen", response_model=StockScreenResponse)
def screen_stocks(
    min_price: Optional[float] = Query(None, description="Minimum current price"),
    ma_50_filter: bool = Query(True, description="Filter stocks above 50-day MA"),
    ma_150_filter: bool = Query(True, description="Filter stocks above 150-day MA"),
    ma_200_filter: bool = Query(True, description="Filter stocks above 200-day MA"),
    limit: int = Query(100, ge=1, le=1000, description="Number of results per page"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    db: Session = Depends(deps.get_db),
):
    """
    Screen stocks based on moving average criteria.

    Returns stocks where current price > 50MA > 150MA > 200MA (configurable filters)
    """
    # Get filtered stocks
    stocks = get_stocks_with_ma_filter(
        db=db,
        min_price=min_price,
        ma_50_filter=ma_50_filter,
        ma_150_filter=ma_150_filter,
        ma_200_filter=ma_200_filter,
        limit=limit,
        offset=offset,
    )

    # Get total count for pagination (simplified for now)
    total = len(stocks)  # This could be optimized with a separate count query

    # Convert to response format with latest prices
    stocks_with_prices = []
    for stock in stocks:
        # Get the latest daily price for this stock
        latest_price = None
        if stock.daily_prices:
            latest_daily_price = max(stock.daily_prices, key=lambda x: x.date)
            latest_price = DailyPriceResponse.model_validate(latest_daily_price)

        stock_with_price = StockWithLatestPrice(
            id=stock.id,
            name=stock.name,
            market=stock.market,
            ticker=stock.ticker,
            latest_price=latest_price,
        )
        stocks_with_prices.append(stock_with_price)

    return StockScreenResponse(
        stocks=stocks_with_prices, total=total, page=offset // limit + 1, limit=limit
    )
