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
import time
import schedule
import datetime

# import github actor
from github.github_actor import GithubActor
import tools.helpers as helpers
from tools.helpers import PipelineType

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

        self.collection_refs = self.set_collection_refs()

    def set_collection_refs(self):
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
                "last_updated": self.db.collection(f"{protocol_name}-last-updated"),
            }

            if not self.db.collection(f"{protocol_name}-widgets").get():
                self.db.collection(f"{protocol_name}-widgets").document("repositories").set({})

            # widgets (collection) -> repositories (doc) -> PROJECT_HASH (sub-collection) -> DATA_NAME (doc) -> 'data': data (field)
            collection_refs["widgets"] = self.db.collection(f"{protocol_name}-widgets")
            logger.info("[*] Collection references are created for protocol {}".format(protocol_name))

        return collection_refs

    def time_until_next_run(self, job):
        """
        Calculate the time (in seconds) until the next run of a job.
        """
        if job.next_run is None:
            return None
        return (job.next_run - datetime.datetime.now()).total_seconds()

    def schedule_tasks(self):
        # Start the jobs immediately upon initialization
        self.crawl_all_protocols()
        self.update_all_protocols()

        # Then schedule the jobs for subsequent runs
        # Schedule crawler to run every 4 weeks
        schedule.every(4).weeks.do(self.crawl_all_protocols).tag("crawl_all_protocols")
        logger.info("[*] Crawler scheduled to run every 4 weeks.")

        # Schedule protocol update to run every week
        schedule.every(1).weeks.do(self.update_all_protocols).tag("update_all_protocols")
        logger.info("[*] Protocol update scheduled to run every week.")

        while True:
            scheduled_crawls = schedule.get_jobs(tag="crawl_all_protocols")
            scheduled_updates = schedule.get_jobs(tag="update_all_protocols")

            if scheduled_crawls:
                next_crawl = self.time_until_next_run(scheduled_crawls[0])
                if next_crawl is not None:  # Ensure it's not None before logging
                    next_crawl_formatted = helpers.format_time_duration(next_crawl)
                    next_crawl_datetime = datetime.datetime.now() + datetime.timedelta(seconds=next_crawl)
                    logger.info(
                        f"[*] Crawling task will run in {next_crawl_formatted}, at {next_crawl_datetime.strftime('%Y-%m-%d %H:%M:%S')}."
                    )
            else:
                logger.info("[!] No crawling task has been scheduled yet.")

            if scheduled_updates:
                next_update = self.time_until_next_run(scheduled_updates[0])
                if next_update is not None:  # Ensure it's not None before logging
                    next_update_formatted = helpers.format_time_duration(next_update)
                    next_update_datetime = datetime.datetime.now() + datetime.timedelta(seconds=next_update)
                    logger.info(
                        f"[*] Update task will run in {next_update_formatted}, at {next_update_datetime.strftime('%Y-%m-%d %H:%M:%S')}."
                    )
            else:
                logger.info("[!] No protocol update task has been scheduled yet.")

            logger.info("[*] Waiting for the next scheduled task...")
            schedule.run_pending()
            time.sleep(60)  # Sleep for a minute before checking again.

    def crawl_all_protocols(self):
        logger.info("[*] Starting the crawling process for all protocols...")
        for protocol in self.protocols:
            if protocol["crawl"] == True:
                protocol_name = protocol["name"]
                crawler_config = protocol["crawler"]
                self.run_crawler(protocol_name, self.collection_refs, crawler_config)
        logger.info("[*] Completed the crawling process for all protocols.")

    def update_all_protocols(self):
        logger.info("[*] Starting the protocol update process for all protocols...")
        for protocol in self.protocols:
            if protocol["update"] == True:
                self.run_protocol_update(protocol, self.collection_refs)
        logger.info("[*] Completed the protocol update process for all protocols.")

    def run_crawler(self, protocol_name, collection_refs, crawler_config):
        self.crawler.run(protocol_name, crawler_config)
        helpers.write_last_updated(collection_refs["last_updated"], PipelineType.CRAWLER.value)

    def run_protocol_update(self, protocol, collection_refs):
        protocol_name = protocol["name"]
        logger.info(f"Updating protocol {protocol_name}")
        ascii_banner = pyfiglet.figlet_format(protocol_name, font="rectangles")
        print(ascii_banner)

        self.pipeline.contruct_pipeline(protocol, collection_refs)
        self.pipeline.run_pipelines()

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
