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
    
    # Moving Averages
    ma_50: Optional[float] = None
    ma_150: Optional[float] = None
    ma_200: Optional[float] = None
    ma_200_20d_ago: Optional[float] = None
    
    # 52-Week High/Low Analysis
    week_52_high: Optional[float] = None
    week_52_low: Optional[float] = None
    is_ma_200_bullish: Optional[bool] = None
    is_near_52w_high: Optional[bool] = None
    is_above_52w_low: Optional[bool] = None
    
    # RS (Relative Strength) Analysis
    roc_252: Optional[float] = Field(None, description="1-year Rate of Change (%)")
    roc_126: Optional[float] = Field(None, description="6-month Rate of Change (%)")
    roc_63: Optional[float] = Field(None, description="3-month Rate of Change (%)")
    roc_21: Optional[float] = Field(None, description="1-month Rate of Change (%)")
    rs_momentum: Optional[float] = Field(None, description="RS Momentum Score")
    rs_rank: Optional[int] = Field(None, description="RS Rank (1 = best)")
    rs_grade: Optional[float] = Field(None, description="RS Grade (0-100, 100 = best)")
    
    class Config:
        from_attributes = True


class RSAnalysisSummary(BaseModel):
    """Summary of RS analysis for quick reference"""
    rs_grade: Optional[float] = Field(None, description="RS Grade (0-100, 100 = best)")
    rs_rank: Optional[int] = Field(None, description="RS Rank (1 = best)")
    rs_momentum: Optional[float] = Field(None, description="RS Momentum Score")
    roc_252: Optional[float] = Field(None, description="1-year performance (%)")
    is_trend_template: Optional[bool] = Field(None, description="Meets trend template criteria")


class StockWithLatestPrice(BaseModel):
    id: int
    name: str
    market: Market
    ticker: str
    latest_price: Optional[DailyPriceResponse] = None
    rs_summary: Optional[RSAnalysisSummary] = None
    
    class Config:
        from_attributes = True


class StockScreenResponse(BaseModel):
    stocks: list[StockWithLatestPrice]
    total: int
    page: int
    limit: int


class PipelineStepResult(BaseModel):
    """Result of a single pipeline step"""
    step_name: str
    success: bool
    duration_seconds: float
    message: str
    details: Optional[dict] = None


class DataPipelineResponse(BaseModel):
    """Response for comprehensive data pipeline operations"""
    pipeline_type: str
    success: bool
    total_duration_seconds: float
    steps: list[PipelineStepResult]
    summary: dict
