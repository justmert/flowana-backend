from datetime import datetime
import json
from typing import Union
from fastapi import FastAPI
import os
import firebase_admin
from firebase_admin import firestore, credentials
from dotenv import load_dotenv
from enum import Enum
import pandas as pd
from collections import defaultdict
from fastapi import HTTPException


load_dotenv()


admin_sdk_path = os.environ['FIREBASE_ADMIN_SDK_PATH']
if not os.path.exists(admin_sdk_path):
    raise Exception(f'Admin SDK file not found in path {admin_sdk_path}')

cred = credentials.Certificate(os.environ['FIREBASE_ADMIN_SDK_PATH'])
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.environ['FIREBASE_ADMIN_SDK_PATH']

app = firebase_admin.initialize_app(cred, {
'projectId': os.environ['FIREBASE_PROJECT_ID']
}, name='flowana_api')

db = firestore.Client()
with open('protocols.json') as f:
    protocols = json.load(f)

app = FastAPI()

@app.get("/")
def read_root():
    return "Hello World!"


@app.get("/protocols/{protocol_name}/commit_activity")
def commit_activity(protocol_name: str, owner: str, repo: str):

    response = db.collection(f'{protocol_name}-widgets').document(f'{owner}#{repo}').get(field_paths=['commit_activity']).to_dict().get('commit_activity',None)
    if response is None:
        return HTTPException(status_code=404, detail="Commit activity not found")
    return response

@app.get("/protocols/{protocol_name}/code_frequency")
def code_frequency(protocol_name: str, owner: str, repo: str):

    response = db.collection(f'{protocol_name}-widgets').document(f'{owner}#{repo}').get(field_paths=['code_frequency']).to_dict().get('code_frequency',None)
    if response is None:
        return HTTPException(status_code=404, detail="Code frequency not found")
    
    return response

@app.get("/protocols/{protocol_name}/commit_participants")
def commit_participation(protocol_name: str, owner: str, repo: str):

    response = db.collection(f'{protocol_name}-widgets').document(f'{owner}#{repo}').get(field_paths=['commit_participation']).to_dict().get('commit_participation',None)
    if response is None:
        return HTTPException(status_code=404, detail="Code frequency not found")
    
    return response

@app.get("/protocols/{protocol_name}/community_profile")
def community_profile(protocol_name: str, owner: str, repo: str):
    response = db.collection(f'{protocol_name}-widgets').document(f'{owner}#{repo}').get(field_paths=['community_profile']).to_dict().get('community_profile',None)
    if response is None:
        return HTTPException(status_code=404, detail="Community profile not found")
    
    return response

@app.get("/protocols/{protocol_name}/punch_card")
def punch_card(protocol_name: str, owner: str, repo: str):
    response = db.collection(f'{protocol_name}-widgets').document(f'{owner}#{repo}').get(field_paths=['punch_card']).to_dict().get('punch_card',None)
    if response is None:
        return HTTPException(status_code=404, detail="Punch card not found")
    
    return response

