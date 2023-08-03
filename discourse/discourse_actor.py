import requests

import os
import time
from time import sleep
import datetime
import logging
import tools.log_config as log_config

logger = logging.getLogger(__name__)


class DiscourseActor:
    @property
    def get_session(self):
        return self.session

    def __init__(self, base_url, session=None):
        self.api_username = os.environ.get("DISCOURSE_API_USERNAME", None)
        self.api_key = os.environ.get("DISCOURSE_API_KEY", None)
        self.discourse_api_endpoint = base_url  # https://forum.onflow.org

        self.github_rest_headers = {
            "Accept": "application/json",
        }
        if self.api_username and self.api_key:
            self.github_rest_headers["Api-Username"] = self.api_username
            self.github_rest_headers["Api-Key"] = self.api_key

        self.session = session
        if not self.session:
            self.session = requests.Session()

    def discourse_rest_make_request(self, url, variables=None, max_page_fetch=float("inf")):
        url = f"{self.discourse_api_endpoint}{url}"
        result = []

        current_fetch_count = 0
        logger.info(f"[GET] fetching data from the url {url}")
        while url and (current_fetch_count < max_page_fetch):
            logger.info(f" [.] Fetching page {current_fetch_count + 1} of {max_page_fetch}")
            self.session.headers.update(self.github_rest_headers)
            response = self.session.get(url, params=variables)
            if response.status_code == 202:
                time.sleep(1)
                logger.info(" [.] Waiting for the data to be ready...")
                continue  # fetch again!

            elif response.status_code == 403:
                self.rate_limit_wait()
                continue

            elif response.status_code == 200:
                json_response = response.json()
                return json_response

            else:
                logger.error(f" [-] Failed to retrieve from API. Status code: {response.status_code}")
                break
        return result
