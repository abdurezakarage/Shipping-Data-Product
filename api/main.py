from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
from .database import SessionLocal
from .schemas import TopProduct, ChannelActivity, MessageSearchResult
from .crud import get_top_products, get_channel_activity, search_messages
from typing import List

app = FastAPI()

# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/")
def read_root():
    return {"message": "Welcome to the Analytical API"}

@app.get("/api/reports/top-products", response_model=List[TopProduct])
def top_products(limit: int = 10, db: Session = Depends(get_db)):
    return get_top_products(db, limit=limit)

@app.get("/api/channels/{channel_name}/activity", response_model=List[ChannelActivity])
def channel_activity(channel_name: str, db: Session = Depends(get_db)):
    return get_channel_activity(db, channel_name=channel_name)

@app.get("/api/search/messages", response_model=List[MessageSearchResult])
def search_messages_endpoint(query: str, db: Session = Depends(get_db)):
    return search_messages(db, query=query) 