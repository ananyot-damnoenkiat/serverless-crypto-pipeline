import functions_framework
import requests
import json
from datetime import datetime
from google.cloud import storage

#Config
BUCKET_NAME = 'crypto-datalake-ananyot-2026'
FILE_PREFIX = 'crypto_data'

@functions_framework.http
def extract_crypto_data(request):
    """
    Extracts cryptocurrency data from a public API.
    """
    try:
        # 1. Extract: Fetch data from a public API.
        url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,dogecoin&vs_currencies=usd,thb"
        response = requests.get(url)
        data = response.json()

        # 2. Transform: Process the data to add a timestamp.
        # Transform data to List of Dicts for easier handling.
        rows = []
        current_time = datetime.utcnow().isoformat()

        for coin, prices in data.items():
            row = {
                "coin": coin,
                "price_usd": prices['usd'],
                "price_thb": prices['thb'],
                "extracted_at": current_time
            }
            rows.append(row)

        # Transform to NDJSON (Newline Delimited JSON). BigQuery friendly format.
        ndjson_data = "\n".join([json.dumps(r) for r in rows])

        # 3. Load: Send the transformed data to Google Cloud Storage (GCS).
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)

        # Create a unique filename based on the current timestamp.
        # Example: crypto_data/2026/01/01/120000_data.json
        dt = datetime.now()
        file_path = f"{FILE_PREFIX}/{dt.year}/{dt.month:02d}/{dt.day:02d}/{dt.strftime('%H%M%S')}_data.json"

        blob = bucket.blob(file_path)
        blob.upload_from_string(ndjson_data, content_type='application/json')

        return f"Success! Data saved to gs://{BUCKET_NAME}/{file_path}", 200
    
    except Exception as e:
        return f"Error: {e}", 500