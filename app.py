import firebase_admin
import requests
import json
from firebase_admin import credentials, firestore

# Setup Firestore db
cred = credentials.Certificate(
    "./serviceAccountKey.json")
default_app = firebase_admin.initialize_app(cred)
db = firestore.client()

# Fetch data from Ontario Public Health API
URL = "https://data.ontario.ca/api/3/action/datastore_search?resource_id=455fd63b-603d-4608-8216-7d8647f43350"
LIMIT = 10000  # Should reflect Max number of cases in Ontario
OFFSET = 0
QUERY = f'{URL}&limit={LIMIT}&offset={OFFSET}'

generic_data_ref = db.collection(u'ontarioData').document(u'generic')

response = requests.get(QUERY).text
response_data = json.loads(response)
results = response_data.get("result")
records = results.get("records")

TOTAL_RESULTS = results.get("total")


# Data transformation and write to db
def writeCovidDataToDb(limit, write_offset):
    data_to_write = []

    for record in records[limit:limit+write_offset]:
        id = record["ROW_ID"]
        episode_date = record["ACCURATE_EPISODE_DATE"]
        age_group = record["Age_Group"]
        gender = record["CLIENT_GENDER"]
        case_outcome = record["OUTCOME1"]
        reporting_phu_address = record["Reporting_PHU_Address"]
        reporting_phu_city = record["Reporting_PHU_City"]
        reporting_phu_latitude = record["Reporting_PHU_Latitude"]
        reporting_phu_longitude = record["Reporting_PHU_Longitude"]
        reporting_phu_postal_code = record["Reporting_PHU_Postal_Code"]

        data_to_write.append({
            "id": id,
            "episode_date": episode_date,
            "age_group": age_group,
            "gender": gender,
            "case_outcome": case_outcome,
            "reporting_phu_address": reporting_phu_address,
            "reporting_phu_city": reporting_phu_city,
            "reporting_phu_latitude": reporting_phu_latitude,
            "reporting_phu_longitude": reporting_phu_longitude,
            "reporting_phu_postal_code": reporting_phu_postal_code,
        })

    # Write to cloud firestore
    generic_data_ref = db.collection(u'ontarioData').document(u'generic')

    if limit == 0:
        generic_data_ref.set({
            "data": data_to_write
        })
    else:
        generic_data_ref.update({
            "data": firestore.ArrayUnion(data_to_write)
        })


# Write to db in chunks of 'WRITE_OFFSET' since firestore does not accept large peices of data
WRITE_OFFSET = 200

for limit in range(0, TOTAL_RESULTS, WRITE_OFFSET):
    writeCovidDataToDb(limit, WRITE_OFFSET)

# To read data from genericData document
# covid_data = generic_data_ref.get()
# covid_data_dict = covid_data.to_dict()
# print(covid_data_dict)
