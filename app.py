import firebase_admin
from firebase_admin import credentials, firestore

cred = credentials.Certificate(
    "./serviceAccountKey.json")

default_app = firebase_admin.initialize_app(cred)

db = firestore.client()

quote = "Yep quote"
author = "Nope author"

doc_ref = db.collection(u'sampleData').document(u'inspiration')
doc_ref.set({
    u'quote': quote,
    u'author': author,
})
