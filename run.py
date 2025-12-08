import os
import pandas as pd
import configparser
import yfinance as yf
from datetime import datetime, timedelta
from pgdb import PGDatabase

# Чтение конфигурации для пути к файлу sales
config = configparser.ConfigParser()
config.read("config.ini")

SALES_PATH = config["Files"]["SALES_PATH"]
COMPANIES = eval(config["Companies"]["COMPANIES"])
DATABASE_CREDS = config["Database"]

sales_df = pd.DataFrame()
if os.path.exists(SALES_PATH):
    sales_df = pd.read_csv(SALES_PATH)
    print(sales_df)
    os.remove(SALES_PATH)

historical_d = {}
for company in COMPANIES:
    data = yf.download(
        tickers=company,
        start="2025-12-01",  # Начальная дата
        end="2025-12-05",    # Конечная дата
        auto_adjust=False
    )

    # Преобразование данных
    if not data.empty:
        data.columns = data.columns.get_level_values(0)
        data.reset_index(inplace=True)
        data['ticker'] = company
        historical_d[company] = data
    else:
        print(f"No data for {company}")

database = PGDatabase (
    host=DATABASE_CREDS['HOST'],
    database=DATABASE_CREDS['DATABASE'],
    user=DATABASE_CREDS['USER'],
    password=DATABASE_CREDS['PASSWORD'],
)

for i, row in sales_df.iterrows():
    query = f"insert into sales values ('{row['dt']}', '{row['company']}', '{row['transaction_type']}', {row['amount']})"
    print(query)
    database.post(query)
    
    
for company, data in historical_d.items():
    for i, row in data.iterrows():
        query = f"insert into stock values ('{row['Date']}', '{row['ticker']}', {row['Open']}, {row['Close']})"
        database.post(query)