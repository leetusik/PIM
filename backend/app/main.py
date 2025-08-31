from app.api.v1 import stocks
from fastapi import FastAPI

app = FastAPI(title="PIM")

app.include_router(
    stocks.router,
    prefix="/api/v1/stocks",
    tags=["Stocks"],
)


@app.get("/")
def read_root():
    return {"message": "Hello World"}
