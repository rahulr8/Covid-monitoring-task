import firebase_admin
import requests
import json
from firebase_admin import credentials, firestore

# Setup Firestore db
cred = credentials.Certificate("./serviceAccountKey.json")
default_app = firebase_admin.initialize_app(cred)
db = firestore.client()


firestore_db_ref = db.collection(u'ontarioData')

# To read data from genericData document
covid_data_collection = firestore_db_ref.stream()
for doc in covid_data_collection:
    print(u'{} => {}'.format(doc.id, doc.to_dict()))
