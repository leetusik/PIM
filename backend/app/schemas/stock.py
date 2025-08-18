import datetime
from typing import Optional

from app.models.stock import Market
from pydantic import BaseModel, Field


class StockCreate(BaseModel):
    name: str = Field(..., description="주식 이름")
    market: Market = Field(..., description="주식 시장")
    ticker: str = Field(..., description="주식 코드")
    # dart_code: str = Field(..., description="Dart 코드")


class DailyPriceCreate(BaseModel):
    stock_id: int = Field(..., description="주식 아이디")
    date: datetime.datetime = Field(..., description="날짜")
    open: float = Field(..., description="시가")
    high: float = Field(..., description="고가")
    low: float = Field(..., description="저가")
    close: float = Field(..., description="종가")
    volume: int = Field(..., description="거래량")


class StockResponse(BaseModel):
    id: int
    name: str
    market: Market
    ticker: str
    
    class Config:
        from_attributes = True


class DailyPriceResponse(BaseModel):
    id: int
    stock_id: int
    date: datetime.date
    open: float
    high: float
    low: float
    close: float
    volume: float
    value: Optional[float] = None
    ma_50: Optional[float] = None
    ma_150: Optional[float] = None
    ma_200: Optional[float] = None
    
    class Config:
        from_attributes = True


class StockWithLatestPrice(BaseModel):
    id: int
    name: str
    market: Market
    ticker: str
    latest_price: Optional[DailyPriceResponse] = None
    
    class Config:
        from_attributes = True


class StockScreenResponse(BaseModel):
    stocks: list[StockWithLatestPrice]
    total: int
    page: int
    limit: int
