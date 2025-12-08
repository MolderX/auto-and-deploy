from datetime import datetime, timedelta
from random import randint
import yfinance as yf
import pandas as pd
import configparser

COMPANIES = ["TSLA", "PFE", "F", "INTC", "BAC"]

today = datetime.today()
yesterday = today - timedelta(days=1)

if 1 <= today.weekday() <=5:
    d = {
        "dt": [yesterday.strftime("%m-%d-%Y")] * len(COMPANIES) * 2,
        "company": COMPANIES * 2,
        "transaction_type": ["buy"] * len(COMPANIES) + ["sell"] * len(COMPANIES),
        "amount": [randint(0,1000) for _ in range(len(COMPANIES) * 2)],
    }
    df = pd.DataFrame(d)
    df.to_csv("sales-data.csv", index=False)