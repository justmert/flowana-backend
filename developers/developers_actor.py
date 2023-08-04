import requests

import os
import time
from time import sleep
import datetime
import logging
import tools.log_config as log_config

logger = logging.getLogger(__name__)


class DevelopersActor:
    @property
    def get_session(self):
        return self.session

    def __init__(self, session=None):
        self.developer_api_endpoint = "https://www.developerreport.com"

        self.developer_rest_headers = {
            "Accept": "application/json",
        }

        self.session = session
        if not self.session:
            self.session = requests.Session()

    def developer_rest_make_request(self, url, variables=None, max_page_fetch=float("inf")):
        url = f"{self.developer_api_endpoint}{url}"
        result = []

        current_fetch_count = 0
        logger.info(f". [=] Fetching data from Graphql API from {self.github_graphql_endpoint}")
        while url and (current_fetch_count < max_page_fetch):
            logger.info(f". page {current_fetch_count + 1}/{max_page_fetch} of {url}")
            self.session.headers.update(self.developer_rest_headers)
            response = self.session.get(url, params=variables)
            if response.status_code == 202:
                logger.info(" [.] Waiting for the data to be ready...")
                time.sleep(1)
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
