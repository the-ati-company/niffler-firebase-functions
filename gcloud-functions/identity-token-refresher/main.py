
import os

from datetime import datetime

from firebase_functions import scheduler_fn, logger
from firebase_admin import initialize_app, credentials, firestore

from google.auth.transport.requests import Request
from google.oauth2.service_account import IDTokenCredentials


cred = credentials.Certificate('service-account.json')

app = initialize_app(cred)

db = firestore.client(app)

cloud_run_service_account_file_path = "could-run-service-account.json"
service_url = os.getenv("CLOUD_RUN_SERVICE_URL")


def refresh_identity():
    credentials = IDTokenCredentials.from_service_account_file(
        cloud_run_service_account_file_path, target_audience=service_url)
    credentials.refresh(Request())
    db.collection("cloud-run-identity").document("token").set(
        {"token": credentials.token,
         "expires": credentials.expiry,
         "updated": datetime.now().isoformat()
         })


@scheduler_fn.on_schedule(schedule="0,20,40 * * * *", timeout_sec=300)
def refresh_identity_token(event):
    logger.log("Identity Token Refresher is running")
    refresh_identity()
    logger.log("Identity Token Refresher is done")
