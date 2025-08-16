import enum

from app.db.base import Base
from sqlalchemy import Column
from sqlalchemy import Enum as SQLAlchemyEnum
from sqlalchemy import Integer, String
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import ForeignKey
from sqlalchemy.types import Date, Float


class Market(enum.Enum):
    KOSPI = "KOSPI"
    KOSDAQ = "KOSDAQ"


class Stock(Base):
    __tablename__ = "stocks"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    market = Column(SQLAlchemyEnum(Market), index=True)
    ticker = Column(String, index=True)
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
    volume = Column(Integer, index=True)

    stock = relationship("Stock", back_populates="daily_prices")
