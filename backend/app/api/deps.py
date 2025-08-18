from app.db.session import SessionLocal


def get_db():
    """API 요청마다 데이터베이스 세션을 생성하고, 끝나면 닫습니다."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
