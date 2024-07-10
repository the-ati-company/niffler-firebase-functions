import requests
import time

from datetime import datetime

from firebase_functions import scheduler_fn, logger
from firebase_admin import initialize_app, credentials, firestore
from firebase_functions.params import IntParam, StringParam

cred = credentials.Certificate('service-account.json')
app = initialize_app(cred)

db = firestore.client(app)


def __get_stock_info(fmp_api_key: str, exchange: str, update_time: str) -> list[dict]:
    link = f"https://financialmodelingprep.com/api/v3/symbol/{exchange}?apikey={fmp_api_key}"
    response = requests.get(link)
    data = response.json()
    parsed_data = []
    for d in data:
        if "symbol" not in d:
            continue
        stock_ticker = d["symbol"]
        stock_name = d["name"] if "name" in d else ""
        stock_price = d["price"] if "price" in d else 0
        parsed_data.append({
            "symbol": stock_ticker,
            "alias": stock_name,
            "price": stock_price,
            "currency": "USD",
            "market": exchange,
            "updated": update_time
        })
    return parsed_data


def __insert_stock_info(stocks: list[dict], exchange: str):
    for stock in stocks:
        symbol = stock["symbol"]
        id = f"{symbol}-{exchange}"
        db.collection('ticker-price').document(id).set(stock)


def get_nyse_stock_info(fmp_api_key: str) -> list[dict]:
    exchange = "NYSE"
    now = datetime.now()
    now = now.strftime("%Y-%m-%d %H:%M:%S")
    parsed_data = __get_stock_info(fmp_api_key, exchange, now)

    __insert_stock_info(parsed_data, exchange)


def get_nasdaq_stock_info(fmp_api_key: str) -> list[dict]:
    exchange = "NASDAQ"
    now = datetime.now()
    now = now.strftime("%Y-%m-%d %H:%M:%S")
    parsed_data = __get_stock_info(fmp_api_key, exchange, now)

    __insert_stock_info(parsed_data, exchange)


def get_amex_stock_info(fmp_api_key: str) -> list[dict]:
    exchange = "AMEX"
    now = datetime.now()
    now = now.strftime("%Y-%m-%d %H:%M:%S")
    parsed_data = __get_stock_info(fmp_api_key, exchange, now)

    __insert_stock_info(parsed_data, exchange)


@scheduler_fn.on_schedule(schedule="0 17 * * 1-5", timezone="America/New_York", timeout_sec=1200)
def us_stock_price_sync(event):
    logger.log("US stock during market scheduler is running")
    FMP_API_KEY = StringParam("FMP_API_KEY")
    logger.log(f"FMP_API_KEY len: {len(FMP_API_KEY.value)}")
    get_amex_stock_info(FMP_API_KEY.value)
    time.sleep(0.1)
    get_nyse_stock_info(FMP_API_KEY.value)
    time.sleep(0.1)
    get_nasdaq_stock_info(FMP_API_KEY.value)
    logger.log("US stock during market scheduler is done")
