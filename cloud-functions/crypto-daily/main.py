import requests

from datetime import datetime

from firebase_functions import scheduler_fn, logger
from firebase_admin import initialize_app, credentials, firestore

from firebase_functions.params import IntParam, StringParam

cred = credentials.Certificate('service-account.json')
app = initialize_app(cred)

db = firestore.client(app)


def __get_crypto_list() -> list[str]:
    return ["BTC",
            "ETH",
            "BNB",
            "XRP",
            "DOGE",
            "SHIB",
            "DOT",
            "LINK",
            "XLM",
            "ATOM"]


def crypto_daily_price_sync():
    cryptos = __get_crypto_list()
    currency = "USD"
    market = "CRYPTO"
    api_key = StringParam("FMP_API_KEY").value
    now = datetime.now()
    now = now.strftime("%Y-%m-%d %H:%M:%S")
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
        id = f"{crypto}-{market}"

        db.collection('ticker-price').document(id).set({
            "symbol": crypto,
            "alias": crypto,
            "price": stock_price,
            "currency": currency,
            "market": market,
            "updated": now
        })


@scheduler_fn.on_schedule(schedule="0 */12 * * *", timeout_sec=100)
def crypto_price_sync(event):
    logger.log("Crypto Daily is running")
    crypto_daily_price_sync()
    logger.log("Crypto Daily is done")
