import requests
import os
import time
import time
import logging
import tools.log_config as log_config

logger = logging.getLogger(__name__)


class GovernanceActor:
    @property
    def get_session(self):
        return self.session

    def __init__(self, session=None):
        self.governance_graphql_endpoint = "https://api.tally.xyz/query"
        self.governance_graphql_headers = {"Api-Key": os.environ["TALLY_API_KEY"]}

        self.governance_rest_endpoint = "https://www.tally.xyz/api"
        self.governance_rest_headers = {
            "Accept": "*/*",
            "Api-Key": os.environ["TALLY_API_KEY"],
        }

        self.session = session
        if not self.session:
            self.session = requests.Session()

    def governance_rest_make_request(self, url, variables=None, max_page_fetch=float("inf")):
        url = f"{self.governance_rest_endpoint}{url}"
        result = []

        current_fetch_count = 0
        logger.info(f". [=] Fetching data from REST API from {url}")
        while url and (current_fetch_count < max_page_fetch):
            logger.info(f". page {current_fetch_count + 1}/{max_page_fetch} of {url}")
            self.session.headers.update(self.governance_rest_headers)
            response = self.session.get(url, params=variables)

            if response.status_code == 200:
                json_response = response.json()
                return json_response

            elif response.status_code == 429:
                logger.warning(". [!] Rate limit exceeded. Waiting for 10 seconds.")
                time.sleep(10)
                continue

            else:
                logger.error(f" [-] Failed to retrieve from API. Status code: {response.status_code} - {response.text}")
                logger.info(f" [#] Rest endpoint: {url}")
                logger.info(f" [#] Variables: {variables}")
                break
        return result

    def governance_graphql_make_query(self, _query, variables=None):
        logger.info(f". [=] Fetching data from Graphql API from {self.governance_graphql_endpoint}")
        self.session.headers.update(self.governance_graphql_headers)
        response = self.session.post(
            self.governance_graphql_endpoint,
            json={"query": _query, "variables": variables},
        )

        if response.status_code in (301, 302):
            response = self.session.post(
                response.headers["location"],
                json={"query": _query, "variables": variables},
            )

        if response.status_code == 200:
            json_response = response.json()
            return json_response

        elif response.status_code == 429:
            logger.warning(". [!] Rate limit exceeded. Waiting for 10 seconds.")
            time.sleep(10)
            return self.governance_graphql_make_query(_query, variables)

        else:
            logger.error(f". [-] Failed to retrieve from API. Status code: {response.status_code} - {response.text}")
            logger.info(f". [#] Graphql endpoint: {self.governance_graphql_endpoint}")
            logger.info(f". [#] Query: {_query}")
            logger.info(f". [#] Variables: {variables}")
            return None
