from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """
    환경 변수를 읽어오는 설정 클래스
    """

    DATABASE_URL: str
    SECRET_KEY: str
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30

    class Config:
        env_file = ".env"


settings = Settings()
