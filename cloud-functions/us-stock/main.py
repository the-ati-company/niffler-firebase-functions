import requests
import time
import json


from datetime import datetime
from typing import Tuple, List, Dict, Any

from firebase_functions import scheduler_fn, logger
from firebase_admin import initialize_app, credentials, firestore
from firebase_functions.params import StringParam

from google.cloud.firestore_v1.base_query import FieldFilter


cred = credentials.Certificate('service-account.json')
app = initialize_app(cred)

db = firestore.client(app)


def __get_stock_info(fmp_api_key: str, exchange: str, update_time: str) -> Tuple[Dict[str, Any], List[str]]:
    link = f"https://financialmodelingprep.com/api/v3/symbol/{exchange}?apikey={fmp_api_key}"
    response = requests.get(link)
    data = response.json()
    parsed_data = {}
    symbols = []

    for d in data:
        if "symbol" not in d:
            continue
        stock_ticker = d["symbol"]
        stock_name = d["name"] if "name" in d else ""
        stock_price = d["price"] if "price" in d else 0
        id = f"{stock_ticker}@{exchange}"
        parsed_data[id] = {
            "symbol": stock_ticker,
            "alias": stock_name,
            "price": stock_price,
            "currency": "USD",
            "market": exchange,
            "updated": update_time
        }
        symbols.append(stock_ticker)
    return parsed_data, symbols


def __insert_stock_info(stocks: Dict[str, Any], exchange: str, symbols: List[str]):
    db.collection(exchange).document("ticker-price").set(
        {"symbols": json.dumps(stocks)})
    db.collection("available_symbols").document(
        exchange).set({"symbols": symbols})


def get_nyse_stock_info(fmp_api_key: str) -> list[dict]:
    exchange = "NYSE"
    now = datetime.now()
    now = now.strftime("%Y-%m-%d %H:%M:%S")
    parsed_data, symbols = __get_stock_info(fmp_api_key, exchange, now)

    __insert_stock_info(parsed_data, exchange, symbols)


def get_nasdaq_stock_info(fmp_api_key: str) -> list[dict]:
    exchange = "NASDAQ"
    now = datetime.now()
    now = now.strftime("%Y-%m-%d %H:%M:%S")
    parsed_data, symbols = __get_stock_info(fmp_api_key, exchange, now)

    __insert_stock_info(parsed_data, exchange, symbols)


def get_amex_stock_info(fmp_api_key: str) -> list[dict]:
    exchange = "AMEX"
    now = datetime.now()
    now = now.strftime("%Y-%m-%d %H:%M:%S")
    parsed_data, symbols = __get_stock_info(fmp_api_key, exchange, now)

    __insert_stock_info(parsed_data, exchange, symbols)


@scheduler_fn.on_schedule(schedule="0 17 * * 1-5", timezone="America/New_York", timeout_sec=1500)
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
