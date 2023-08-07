import requests
import os
import time
import logging
import tools.log_config as log_config

logger = logging.getLogger(__name__)


class MessariActor:
    @property
    def get_session(self):
        return self.session

    def __init__(self, session=None):
        self.messari_api_endpoint = "https://data.messari.io/api"

        api_key = os.environ.get("MESSARI_API_KEY", None)
        if api_key is None:
            raise ValueError("MESSARI_API_KEY is not set")

        self.messari_rest_headers = {
            "Accept": "application/json",
            "x-messari-api-key": api_key,
        }
        self.session = session
        if not self.session:
            self.session = requests.Session()

    def messari_rest_make_request(self, url, variables=None, max_page_fetch=float("inf"), server_fail_max_try=3):
        url = f"{self.messari_api_endpoint}{url}"
        result = []

        server_fail_current_try = 0
        current_fetch_count = 0
        logger.info(f". [=] Fetching data from REST API from {url}")
        while url and (current_fetch_count < max_page_fetch):
            logger.info(f". page {current_fetch_count + 1}/{max_page_fetch} of {url}")
            self.session.headers.update(self.messari_rest_headers)
            response = self.session.get(url, params=variables)
            if response.status_code == 429:
                logger.info(". [...] Rate limit exceeded. Waiting for 1 minute.")
                time.sleep(1 * 60)
                continue  # fetch again!

            elif response.status_code == 200:
                json_response = response.json()
                return json_response

            elif response.status_code == 500:
                logger.info(". [!] Server error. Let's try again.")
                server_fail_current_try += 1
                if server_fail_current_try < server_fail_max_try:
                    time.sleep(1.1)
                    continue
                else:
                    logger.error(f" [-] Server error. Tried {server_fail_max_try} times. Giving up.")
            else:
                logger.error(f" [-] Failed to retrieve from API. Status code: {response.status_code} - {response.text}")
                logger.info(f" [#] Rest endpoint: {url}")
                logger.info(f" [#] Variables: {variables}")
                break
        return result
