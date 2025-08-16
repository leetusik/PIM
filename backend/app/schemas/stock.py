import datetime

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
