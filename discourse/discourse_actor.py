import requests
import os
import time
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

        self.discourse_rest_headers = {
            "Accept": "application/json",
        }
        if self.api_username and self.api_key:
            self.discourse_rest_headers["Api-Username"] = self.api_username
            self.discourse_rest_headers["Api-Key"] = self.api_key

        self.session = session
        if not self.session:
            self.session = requests.Session()

    def discourse_rest_make_request(self, url, variables=None, max_page_fetch=float("inf")):
        url = f"{self.discourse_api_endpoint}{url}"
        result = []

        current_fetch_count = 0
        logger.info(f". [=] Fetching data from REST API from {url}")
        while url and (current_fetch_count < max_page_fetch):
            logger.info(f". page {current_fetch_count + 1}/{max_page_fetch} of {url}")
            self.session.headers.update(self.discourse_rest_headers)
            response = self.session.get(url, params=variables)
            
            if response.status_code == 200:
                json_response = response.json()
                return json_response
            
            elif response.status_code == 429:
                logger.warning(". [...] Rate limit reached. Sleeping for 10 seconds.")
                time.sleep(10)
                continue

            else:
                logger.error(f" [-] Failed to retrieve from API. Status code: {response.status_code} - {response.text}")
                logger.info(f" [#] Rest endpoint: {url}")
                logger.info(f" [#] Variables: {variables}")
                break
        return result
