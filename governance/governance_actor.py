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
            if response.status_code == 202:
                time.sleep(1.1)
                logger.info(". [...] Waiting for the data to be ready.")
                continue  # fetch again!

            elif response.status_code == 403:
                time.sleep(1.1)
                continue

            elif response.status_code == 200:
                json_response = response.json()
                return json_response

            else:
                logger.error(f" [-] Failed to retrieve from API. Status code: {response.status_code} - {response.text}")
                logger.info(f" [#] Rest endpoint: {url}")
                logger.info(f" [#] Variables: {variables}")
                break
        return result

    def governance_graphql_make_query(self, _query, variables=None):
        logger.info(f". [=] Fetching data from Graphql API from {self.github_graphql_endpoint}")
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

        if response.status_code != 200:
            logger.error(f". [-] Failed to retrieve from API. Status code: {response.status_code} - {response.text}")
            logger.info(f". [#] Graphql endpoint: {self.github_graphql_endpoint}")
            logger.info(f". [#] Query: {_query}")
            logger.info(f". [#] Variables: {variables}")
            return None

        elif response.status_code == 403:
            logger.info(". [...] Too many request. Waiting for the data to be ready.")
            time.sleep(1.1)
            return self.governance_graphql_make_query(_query, variables)

        return response.json()
