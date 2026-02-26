"""Entry point for the Arbitrage Insights dashboard.

Run with:
    python run.py

Or directly via uvicorn:
    uvicorn app.main:app --host 127.0.0.1 --port 8000
"""
from app.main import run

if __name__ == "__main__":
    run()
