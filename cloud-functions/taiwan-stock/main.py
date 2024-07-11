import requests
import json
import time

from datetime import datetime

from firebase_functions import scheduler_fn, logger
from firebase_admin import initialize_app, credentials, firestore

from google.cloud.firestore_v1.base_query import FieldFilter

from typing import Tuple, List, Dict, Any

cred = credentials.Certificate('service-account.json')
app = initialize_app(cred)

db = firestore.client(app)


def __parse_float(float_str: str):
    try:
        return float(float_str.replace('"', ''))
    except ValueError:
        return 0


def __insert_stock_info(stocks: Dict[str, Any], exchange: str, symbols: List[str]):
    len_stocks = len(stocks.keys())
    parsed_dicts = []
    if len_stocks > 5000:
        temp = {}
        for id, value in stocks.items():
            temp[id] = value
            if len(temp) == 5000:
                parsed_dicts.append(temp)
                temp = {}
        if len(temp) > 0:
            parsed_dicts.append(temp)
        stocks = parsed_dicts
    else:
        stocks = [stocks]

    # remove the extra documents
    len_docs = len(stocks)
    docs = db.collection(exchange).stream()
    count = sum(1 for _ in docs)
    for i in range(count):
        if i > len_docs - 1:
            db.collection(exchange).document(f"ticker-price-{i}").delete()

    time.sleep(1)

    # adding the new documents
    for i in range(len(stocks)):
        db.collection(exchange).document(f"ticker-price-{i}").set(
            {"symbols": json.dumps(stocks[i], ensure_ascii=False)})
    db.collection("available_symbols").document(
        exchange).set({"symbols": symbols})


def get_twse_stock_info() -> Tuple[Dict[str, Any], List[str]]:
    link = 'https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL'
    response = requests.get(link)
    data = response.json()
    data = data["data"]
    parsed_data = {}
    symbols = []
    now = datetime.now()
    now = now.strftime("%Y-%m-%d %H:%M:%S")
    logger.log(f"Updating {len(data)} twse stocks")
    for d in data:
        if len(d) < 9:
            continue
        stock_ticker = d[0].replace('"', '').strip()
        stock_name = d[1].replace('"', '').strip()
        stock_price = __parse_float(d[7].strip())
        id = f"{stock_ticker}@TWSE"
        parsed_data[id] = {
            "symbol": stock_ticker,
            "alias": stock_name,
            "price": stock_price,
            "currency": "TWD",
            "market": "TWSE",
            "updated": now
        }
        symbols.append(stock_ticker)
    return parsed_data, symbols


def get_tpex_stock_info() -> Tuple[Dict[str, Any], List[str]]:

    link = 'https://www.tpex.org.tw/openapi/v1/tpex_esb_latest_statistics'
    response = requests.get(link)
    data = response.json()
    parsed_data = {}
    symbols = []
    now = datetime.now()
    now = now.strftime("%Y-%m-%d %H:%M:%S")
    logger.log(f"Updating {len(data)} tpex stocks")
    for d in data:
        if "SecuritiesCompanyCode" not in d:
            continue
        stock_ticker = d["SecuritiesCompanyCode"]
        stock_name = d["CompanyName"] if "CompanyName" in d else ""
        stock_price = __parse_float(
            d["LatestPrice"]) if "LatestPrice" in d else 0
        id = f"{stock_ticker}@TPEX"
        parsed_data[id] = {
            "symbol": stock_ticker,
            "alias": stock_name,
            "price": stock_price,
            "currency": "TWD",
            "market": "TPEX",
            "updated": now
        }
        symbols.append(stock_ticker)
    return parsed_data, symbols


def get_taiwan_stock_price():
    stocks, symbols = get_twse_stock_info()
    __insert_stock_info(stocks, "TWSE", symbols)
    stocks, symbols = get_tpex_stock_info()
    __insert_stock_info(stocks, "TPEX", symbols)


@scheduler_fn.on_schedule(schedule="0,15,30,45 8-17 * * 1-5", timezone="Asia/Taipei", timeout_sec=800)
def taiwan_stock_price_sync(event):
    logger.log("Taiwan stock during market scheduler is running")
    get_taiwan_stock_price()
    logger.log("Taiwan stock during market scheduler is done")
