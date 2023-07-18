import os
import logging
import log_config
import firebase_admin
from firebase_admin import firestore, credentials
import os 
import json
import pyfiglet
from requests_cache import CachedSession
from github_actor import GithubActor
from discourse_actor import DiscourseActor
from crawler import Crawler
import requests
import api
from datetime import timedelta
from pipeline import Pipeline

logger = logging.getLogger(__name__)


class Flowana():
    @property
    def get_db(self):
        return self.db

    @property
    def get_app(self):
        return self.app

    @property
    def get_actor(self):
        return self.github_actor

    def __init__(self):
        ascii_banner = pyfiglet.figlet_format("Flowana")
        print(ascii_banner)
        logger.info('Initializing Flowana')

        admin_sdk_path = os.environ['FIREBASE_ADMIN_SDK_PATH']
        if not os.path.exists(admin_sdk_path):
            raise Exception(f'Admin SDK file not found in path {admin_sdk_path}')
        
        cred = credentials.Certificate(os.environ['FIREBASE_ADMIN_SDK_PATH'])
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.environ['FIREBASE_ADMIN_SDK_PATH']

        self.app = firebase_admin.initialize_app(cred, {
        'projectId': os.environ['FIREBASE_PROJECT_ID']
        }, name='flowana-backend')
        
        self.db = firestore.Client()
        with open('protocols.json') as f:
            self.protocols = json.load(f)

        self.session = CachedSession('session', backend='sqlite', expire_after=timedelta(days=30))
        self.github_actor = GithubActor(self.session) # do not need caching actor for crawler


        self.crawler = Crawler(self.app, self.db, self.github_actor) 
        self.pipeline = Pipeline(self.app, self.db, self.github_actor)        
        logger.info(f"Loaded protocols {','.join([protocol['name'].upper() for protocol in self.protocols])}.")


    def check_collection(self, collection_ref):
        # Check if the collection exists
        if not collection_ref.get():
            # Create a dummy document to force the collection to be created
            collection_ref.document('dummy').set({})
 
    # Delete any dummy documents that were created
    def delete_dummy_docs(self, collection_ref):
        dummy_docs = collection_ref.where('dummy', '==', True).get()
        for doc in dummy_docs:
            doc.reference.delete()

    def run(self):
        for protocol in self.protocols:
            collection_refs = {'projects': self.db.collection(f'{protocol["name"]}-projects'),
                                'widgets': self.db.collection(f'{protocol["name"]}-widgets'), 
                                'cumulative': self.db.collection(f'{protocol["name"]}-cumulative'),
                                'discourse': self.db.collection(f'{protocol["name"]}-discourse'),
                                'developers': self.db.collection(f'{protocol["name"]}-developers'),
                                }

            # Create the collections if they don't exist
            [self.check_collection(collection_ref) for collection_ref in collection_refs.values()]
            if protocol['crawl'] == True:
                self.run_crawler(protocol['name'], protocol['crawler_tomls'])
                self.session.cache.clear()

            if protocol['update'] == True:
                self.run_protocol_update(protocol, collection_refs)

            self.session.cache.clear()
            # Delete the dummy documents
            [self.delete_dummy_docs(collection_ref) for collection_ref in collection_refs.values()]

    def run_crawler(self, protocol_name, tomls):
        logger.info(f"Crawling protocol {protocol_name} with [{','.join(tomls)}]] tomls.")
        self.crawler.run(protocol_name, tomls)

    def run_protocol_update(self, protocol, collection_refs):
        protocol_name = protocol['name']
        logger.info(f"Updating protocol {protocol_name}")
        ascii_banner = pyfiglet.figlet_format(protocol_name)
        print(ascii_banner)

        self.pipeline.contruct_pipeline(protocol, collection_refs)
        self.pipeline.run_pipelines()
