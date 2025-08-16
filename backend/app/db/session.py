from app.core.config import settings
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

# Base 모델 (모든 ORM 모델의 부모 클래스)
Base = declarative_base()

# 데이터베이스 엔진 생성
engine = create_engine(settings.DATABASE_URL)

# 세션 생성기
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
