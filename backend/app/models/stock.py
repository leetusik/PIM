import enum

from app.db.session import Base
from sqlalchemy import Column
from sqlalchemy import Enum as SQLAlchemyEnum
from sqlalchemy import Integer, String
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import ForeignKey, UniqueConstraint
from sqlalchemy.types import Boolean, Date, Float


class Market(enum.Enum):
    KOSPI = "KOSPI"
    KOSDAQ = "KOSDAQ"


class Stock(Base):
    __tablename__ = "stocks"

    id = Column(Integer, primary_key=True, index=True, nullable=False)
    name = Column(String, index=True, unique=True, nullable=False)
    market = Column(SQLAlchemyEnum(Market), index=True, nullable=False)
    ticker = Column(String, index=True, unique=True, nullable=False)
    # dart_code = Column(String, index=True)

    daily_prices = relationship("DailyPrice", back_populates="stock")


class DailyPrice(Base):
    __tablename__ = "daily_price"

    id = Column(Integer, primary_key=True, index=True)
    stock_id = Column(Integer, ForeignKey("stocks.id"))
    date = Column(Date, index=True)
    open = Column(Float, index=True)
    high = Column(Float, index=True)
    low = Column(Float, index=True)
    close = Column(Float, index=True)
    volume = Column(Float, index=True)
    value = Column(Float, index=True)

    ma_50 = Column(Float, index=True)
    ma_150 = Column(Float, index=True)
    ma_200 = Column(Float, index=True)
    ma_200_20d_ago = Column(Float, index=True)
    is_ma_200_bullish = Column(Boolean, index=True)
    week_52_high = Column(Float, index=True)
    is_near_52w_high = Column(Boolean, index=True)
    week_52_low = Column(Float, index=True)
    is_above_52w_low = Column(Boolean, index=True)

    stock = relationship("Stock", back_populates="daily_prices")

    __table_args__ = (UniqueConstraint("stock_id", "date", name="unique_stock_date"),)
