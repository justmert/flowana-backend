import os
import logging
import log_config
import firebase_admin
from firebase_admin import firestore, credentials
import os 
import json
import pyfiglet
from requests_cache import CachedSession
from request_actor import Actor
from crawler import Crawler
import requests
import api

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
        return self.actor

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
        }, os.environ['FIREBASE_PROJECT_ID'])
        
        self.db = firestore.Client()
        with open('protocols.json') as f:
            self.protocols = json.load(f)

        self.session = CachedSession('session', backend='sqlite', expire_after=3600)
        self.actor = Actor(self.session) # do not need caching actor for crawler
        self.crawler = Crawler(self.app, self.db, self.actor) 

        logger.info(f"Loaded protocols {','.join([protocol['name'].upper() for protocol in self.protocols])}.")

         
    def run(self):
        for protocol in self.protocols:
            if protocol['crawl'] == True:
                self.run_crawler(protocol['name'], protocol['crawler_tomls'])
                self.session.cache.clear()

            if protocol['update'] == True:
                self.run_protocol_update(protocol['name'])
                self.session.cache.clear()

    def run_crawler(self, protocol_name, tomls):
        logger.info(f"Crawling protocol {protocol_name} with [{','.join(tomls)}]] tomls.")
        self.crawler.run(protocol_name, tomls)

    def run_protocol_update(self, protocol_name):
        logger.info(f"Updating protocol {protocol_name}")
        ascii_banner = pyfiglet.figlet_format(protocol_name)
        print(ascii_banner)
 