from fastapi import FastAPI

app = FastAPI(title="PIM")


@app.get("/")
def read_root():
    return {"message": "Hello World"}
