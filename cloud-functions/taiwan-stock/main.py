import requests

from datetime import datetime

from firebase_functions import scheduler_fn, logger
from firebase_admin import initialize_app, credentials, firestore

cred = credentials.Certificate('service-account.json')
app = initialize_app(cred)

db = firestore.client(app)


def parse_float(float_str: str):
    try:
        return float(float_str.replace('"', ''))
    except ValueError:
        return 0


def get_twse_stock_info() -> list[dict]:
    link = 'https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL'
    response = requests.get(link)
    data = response.json()
    data = data["data"]
    parsed_data = []
    now = datetime.now()
    now = now.strftime("%Y-%m-%d %H:%M:%S")
    for d in data:
        if len(d) < 9:
            continue
        stock_ticker = d[0].replace('"', '').strip()
        stock_name = d[1].replace('"', '').strip()
        stock_price = parse_float(d[7].strip())
        parsed_data.append({
            "symbol": stock_ticker,
            "alias": stock_name,
            "price": stock_price,
            "currency": "TWD",
            "market": "TWSE",
            "updated": now
        })
    return parsed_data


def get_tpex_stock_info():

    link = 'https://www.tpex.org.tw/openapi/v1/tpex_esb_latest_statistics'
    response = requests.get(link)
    data = response.json()
    parsed_data = []
    now = datetime.now()
    now = now.strftime("%Y-%m-%d %H:%M:%S")
    for d in data:
        if "SecuritiesCompanyCode" not in d:
            continue
        stock_ticker = d["SecuritiesCompanyCode"]
        stock_name = d["CompanyName"] if "CompanyName" in d else ""
        stock_price = parse_float(
            d["LatestPrice"]) if "LatestPrice" in d else 0
        parsed_data.append({
            "symbol": stock_ticker,
            "alias": stock_name,
            "price": stock_price,
            "currency": "TWD",
            "market": "TPEX",
            "updated": now
        })
    return parsed_data


def __taiwan_stock_price_sync():
    stocks = get_twse_stock_info()

    logger.log(f"Updating {len(stocks)} twse stocks")
    for stock in stocks:
        id = f"{stock['symbol']}-TWSE"
        db.collection(
            'ticker-price').document(id).set(stock)

    stocks = get_tpex_stock_info()
    logger.log(f"Updating {len(stocks)} tpex stocks")
    for stock in stocks:
        id = f"{stock['symbol']}-TPEX"
        db.collection(
            'ticker-price').document(id).set(stock)


@scheduler_fn.on_schedule(schedule="40 15 * * 1-5", timezone="Asia/Taipei", timeout_sec=800)
def taiwan_stock_price_sync(event):
    logger.log("Taiwan stock during market scheduler is running")
    __taiwan_stock_price_sync()
    logger.log("Taiwan stock during market scheduler is done")
