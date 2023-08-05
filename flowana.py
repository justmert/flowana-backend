import os
import logging
import firebase_admin
from firebase_admin import firestore, credentials
import os
import json
import pyfiglet
from crawler.crawler import Crawler
from pipeline import Pipeline
import tools.log_config as log_config

# import github actor
from github.github_actor import GithubActor

current_file_path = os.path.abspath(__file__)
base_dir = os.path.dirname(current_file_path)
logger = logging.getLogger(__name__)


class Flowana:
    @property
    def get_db(self):
        return self.db

    @property
    def get_app(self):
        return self.app

    def __init__(self):
        ascii_banner = pyfiglet.figlet_format("Flowana", font="big")
        print(ascii_banner)
        logger.info("[...] Initializing Flowana.")

        admin_sdk_path = os.path.join(base_dir, os.environ["FIREBASE_ADMIN_SDK_NAME"])
        if not os.path.exists(admin_sdk_path):
            raise Exception(f"Admin SDK file not found in path {admin_sdk_path}")

        cred = credentials.Certificate(admin_sdk_path)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = admin_sdk_path

        self.app = firebase_admin.initialize_app(
            cred,
            {"projectId": os.environ["FIREBASE_PROJECT_ID"]},
            name="flowana-backend",
        )
        logger.info("[*] Firebase app initialized.")

        self.db = firestore.Client()
        with open("protocols.json") as f:
            self.protocols = json.load(f)

        logger.info("[*] Loaded protocols. There are {} protocols.".format(len(self.protocols)))

        self.github_actor = GithubActor()
        self.crawler = Crawler(self.app, self.db, self.github_actor)
        self.pipeline = Pipeline(self.app, self.db, self.github_actor)

    def check_collection(self, collection_ref):
        # Check if the collection exists
        if not collection_ref.get():
            # Create a dummy document to force the collection to be created
            collection_ref.document("dummy").set({})

    # Delete any dummy documents that were created
    def delete_dummy_docs(self, collection_ref):
        dummy_docs = collection_ref.where("dummy", "==", True).get()
        for doc in dummy_docs:
            doc.reference.delete()

    def _create_dummies(self, collection_refs):
        # Create the collections if they don't exist
        [self.check_collection(collection_ref) for collection_ref in collection_refs.values()]
        logger.info("[*] Created dummy documents.")

    def _delete_dummies(self, collection_refs):
        # Delete the dummy documents
        [self.delete_dummy_docs(collection_ref) for collection_ref in collection_refs.values()]
        logger.info("[*] Deleted dummy documents.")

    def run(self):
        for protocol in self.protocols:
            protocol_name = protocol["name"]
            collection_refs = {
                "projects": self.db.collection(f"{protocol_name}-projects"),
                "cumulative": self.db.collection(f"{protocol_name}-cumulative"),
                "leaderboard": self.db.collection(f"{protocol_name}-leaderboard"),
                "discourse": self.db.collection(f"{protocol_name}-discourse"),
                "developers": self.db.collection(f"{protocol_name}-developers"),
                "governance": self.db.collection(f"{protocol_name}-governance"),
                "messari": self.db.collection(f"{protocol_name}-messari"),
            }

            if not self.db.collection(f"{protocol_name}-widgets").get():
                self.db.collection(f"{protocol_name}-widgets").document("repositories").set({})

            # widgets (collection) -> repositories (doc) -> PROJECT_HASH (sub-collection) -> DATA_NAME (doc) -> 'data': data (field)
            collection_refs["widgets"] = self.db.collection(f"{protocol_name}-widgets")

            logger.info("[*] Collection references are created for protocol {}".format(protocol_name))

            if protocol["crawl"] == True:
                self.run_crawler(protocol_name, protocol["crawler"])

            if protocol["update"] == True:
                self.run_protocol_update(protocol, collection_refs)

    def run_crawler(self, protocol_name, crawler_config):
        self.crawler.run(protocol_name, crawler_config)

    def run_protocol_update(self, protocol, collection_refs):
        protocol_name = protocol["name"]
        logger.info(f"Updating protocol {protocol_name}")
        ascii_banner = pyfiglet.figlet_format(protocol_name, font="rectangles")
        print(ascii_banner)

        self.pipeline.contruct_pipeline(protocol, collection_refs)
        self.pipeline.run_pipelines()
