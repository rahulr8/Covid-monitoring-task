import firebase_admin
import requests
import json
from firebase_admin import credentials, firestore

# Setup Firestore db
cred = credentials.Certificate(
    "./serviceAccountKey.json")
default_app = firebase_admin.initialize_app(cred)
db = firestore.client()

URL = "https://data.ontario.ca/api/3/action/datastore_search?resource_id=455fd63b-603d-4608-8216-7d8647f43350"
LIMIT = 5
OFFSET = 0

QUERY = f'{URL}&limit={LIMIT}&offset={OFFSET}'

response = requests.get(QUERY).text
response_data = json.loads(response)

print(json.dumps(response_data, indent=4))

# quote = "Yep quote"
# author = "Nope author"

# doc_ref = db.collection(u'sampleData').document(u'inspiration')
# doc_ref.set({
#     u'quote': quote,
#     u'author': author,
# })

# doc = doc_ref.get()
# print(doc.to_dict())
