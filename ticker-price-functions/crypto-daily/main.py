import requests
import json
import time

from datetime import datetime
from typing import Tuple, List, Dict, Any

from firebase_functions import scheduler_fn, logger
from firebase_admin import initialize_app, credentials, firestore

from firebase_functions.params import StringParam

from google.cloud.firestore_v1.base_query import FieldFilter

cred = credentials.Certificate('service-account.json')
app = initialize_app(cred)

db = firestore.client(app)


def __get_crypto_list() -> list[str]:
    return ["BTC",
            "BNB",
            "XRP",
            "DOGE",
            "DOT",
            "LINK",
            "LTC",
            "BCH",
            "XLM",
            "ATOM",
            "SOL",
            "KAU",
            "KAG"]


def __insert_stock_info(stocks: Dict[str, Any], exchange: str, symbols: Dict[str, List[str]]):
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

    time.sleep(0.02)

    # adding the new documents
    for i in range(len(stocks)):
        db.collection(exchange).document(f"ticker-price-{i}").set(
            {"symbols": json.dumps(stocks[i], ensure_ascii=False)})
    db.collection("available_symbols").document(
        exchange).set({"symbols": symbols})


def get_crypto_daily_price():
    cryptos = __get_crypto_list()
    currency = "USD"
    market = "CRYPTO"
    api_key = StringParam("FMP_API_KEY").value
    now = datetime.now()
    now = now.strftime("%Y-%m-%d %H:%M:%S")
    parsed_data = {}
    symbols = []
    for crypto in cryptos:
        pair = f"{crypto}{currency}"
        base_link = f"https://financialmodelingprep.com/api/v3/historical-price-full/{pair}"
        link = f"{base_link}?apikey={api_key}"
        response = requests.get(link)

        if response.status_code != 200:
            logger.log(f"Failed to get data from {base_link}")
            continue

        data = response.json()
        if "historical" not in data or len(data["historical"]) == 0:
            logger.log(f"Failed to get historical data from {base_link}")
            continue

        data = data["historical"][0]
        stock_price = data["close"]

        id = f"{crypto}@{market}"

        parsed_data[id] = {
            "symbol": crypto,
            "alias": crypto,
            "price": stock_price,
            "currency": currency,
            "market": market,
            "updated": now
        }

        symbols.append(crypto)

    __insert_stock_info(parsed_data, market, symbols)


@scheduler_fn.on_schedule(schedule="0 */2 * * *", timeout_sec=300, memory=1024)
def crypto_price_sync(event):
    logger.log("Crypto Daily is running")
    get_crypto_daily_price()
    logger.log("Crypto Daily is done")
