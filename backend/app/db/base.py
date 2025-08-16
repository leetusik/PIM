# 이 파일은 모든 모델을 임포트하여 Alembic이 인식할 수 있도록 합니다.
from app.db.session import Base

# Import all models here for Alembic auto-detection
from app.models.stock import Stock  # noqa: F401
