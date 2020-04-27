import firebase_admin
import requests
import json

from firebase_admin import credentials, firestore
from writeToDb import writeCovidDataToDb

# Setup Firestore db
cred = credentials.Certificate("./serviceAccountKey.json")
default_app = firebase_admin.initialize_app(cred)
db = firestore.client()

# Fetch data from Ontario Public Health API
URL = "https://data.ontario.ca/api/3/action/datastore_search?resource_id=455fd63b-603d-4608-8216-7d8647f43350"
LIMIT = 10000  # Should reflect Max number of cases in Ontario
OFFSET = 0
QUERY = f'{URL}&limit={LIMIT}&offset={OFFSET}'

response = requests.get(QUERY).text
response_data = json.loads(response)
results = response_data.get("result")

recordsFromFirestore = results.get("records")
TOTAL_RESULTS = results.get("total")


# Write to db in chunks of 'WRITE_OFFSET' since firestore does not accept large pieces of data
WRITE_OFFSET = 500

for limit in range(0, TOTAL_RESULTS, WRITE_OFFSET):
    writeCovidDataToDb(
        limit,
        WRITE_OFFSET,
        TOTAL_RESULTS,
        recordsFromFirestore,
        db
    )
