from app.db.base import Base
from sqlalchemy import Column, Integer, String


class Stock(Base):
    __tablename__ = "stocks"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    market = Column(String, index=True)
    ticker = Column(String, index=True)
