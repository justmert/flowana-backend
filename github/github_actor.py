import requests
import os
import time
from time import sleep
import logging
import tools.log_config as log_config

logger = logging.getLogger(__name__)


class GithubActor:
    @property
    def get_session(self):
        return self.session

    def __init__(self, session=None):
        self.bearer_key = os.environ["GITHUB_BEARER_KEY"]
        self.github_graphql_endpoint = "https://api.github.com/graphql"
        self.github_graphql_headers = {"Authorization": f"Bearer {self.bearer_key}"}

        self.github_rest_endpoint = "https://api.github.com"
        self.github_rest_headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "Authorization": f"Bearer {self.bearer_key}",
        }
        self.session = session
        if not self.session:
            self.session = requests.Session()

    def github_graphql_make_query(self, _query, variables=None):
        logger.info(f". [=] Fetching data from Graphql API from {self.github_graphql_endpoint}")
        self.session.headers.update(self.github_graphql_headers)
        response = self.session.post(
            self.github_graphql_endpoint,
            json={"query": _query, "variables": variables},
        )

        if response.status_code == 200:
            json_response = response.json()
            return json_response

        elif response.status_code in (301, 302):
            response = self.session.post(
                response.headers["location"],
                json={"query": _query, "variables": variables},
            )

        elif response.status_code == 403:
            self.rate_limit_wait()
            return self.github_graphql_make_query(_query, variables)

        else:
            logger.error(f". [-] Failed to retrieve from API. Status code: {response.status_code} - {response.text}")
            logger.info(f". [#] Graphql endpoint: {self.github_graphql_endpoint}")
            logger.info(f". [#] Query: {_query}")
            logger.info(f". [#] Variables: {variables}")
            return None


    def github_rest_make_request(self, url, variables=None, max_page_fetch=float("inf")):
        url = f"{self.github_rest_endpoint}{url}"
        logger.info(f". [=] Fetching data from REST API from {url}")

        result = []
        current_fetch_count = 0
        while url and (current_fetch_count < max_page_fetch):
            logger.info(f". page {current_fetch_count + 1}/{max_page_fetch} of {url}")
            self.session.headers.update(self.github_rest_headers)
            response = self.session.get(url, params=variables)

            if response.status_code == 202:
                logger.info(". [...] Waiting for the data to be ready.")
                time.sleep(1)
                continue  # fetch again!

            elif response.status_code == 403:
                self.rate_limit_wait()
                continue

            elif response.status_code == 200:
                json_response = response.json()
                url = response.links.get("next", {}).get("url", None)
                if max_page_fetch == 1:
                    return json_response

                if isinstance(json_response, list):
                    result.extend(json_response)

                else:
                    result.append(json_response)

                if url is None:
                    if current_fetch_count == 0:
                        return json_response

                current_fetch_count += 1
            else:
                logger.error(f" [-] Failed to retrieve from API. Status code: {response.status_code} - {response.text}")
                logger.info(f" [#] Rest endpoint: {url}")
                logger.info(f" [#] Variables: {variables}")
                break
        return result

    def rate_limit_wait(self, rate_limit_reset):
        reset_time = int(rate_limit_reset)
        time_to_wait = reset_time - int(time.time())
        logger.warning(f". [-] Waiting for {time_to_wait} seconds which is {round(time_to_wait / 60, 2)} minutes.")
        time.sleep(time_to_wait)

    def check_repo_validity(self, owner, repo, try_val=0):
        # Make an HTTP request to the GitHub API endpoint for the repository
        response = requests.get(f"https://api.github.com/repos/{owner}/{repo}")

        # Check if the response is successful or if the repository is inaccessible
        if response.status_code == 200:
            logger.info(f". [*] The repository {owner}/{repo} is safely accessible.")
            return response.json()

        elif response.status_code == 202:
            logger.info(f". [-] The request was successful and there is no response body. Trying again.")
            sleep(1.5)  # Wait for 1 second
            if try_val > 5:
                logger.warning(f". [-] Too many tries. Aborting.")
                return False
            return self.check_repo_validity(owner, repo, try_val + 1)

        elif response.status_code == 400:
            logger.warning(f". [-] Bad request.")
            return False

        elif response.status_code == 401:
            logger.warning(f". [-] The repository {repo} is private and the user is not authenticated.")
            return False

        elif response.status_code == 403:
            logger.warning(f". [-] The user has exceeded the rate limit and needs to wait before making more requests.")
            self.rate_limit_wait(response.headers["x-ratelimit-reset"])
            return self.check_repo_validity(owner, repo, try_val + 1)

        elif response.status_code == 404:
            logger.warning(f". [-] The repository {repo} does not exist.")
            return False

        elif response.status_code == 410:
            logger.warning(f". [-] The repository {repo} has been deleted.")
            return False

        elif response.status_code == 422:
            logger.warning(f". [-] The request was well-formed but was unable to be followed due to semantic errors.")
            return False

        elif response.status_code == 429:
            logger.warning(f". [-] The user has sent too many requests in a given amount of time.")
            sleep(1)  # Wait for 1 second
            if try_val > 5:
                logger.warning(f". [-] Too many tries. Aborting.")
                return False
            return self.check_repo_validity(owner, repo, try_val + 1)

        elif response.status_code == 500:
            logger.warning(f". [-] An error occurred on the server.")
            return False

        else:
            logger.warning(f". [-] Unknown error [{response.status_code}] - {response.reason}: {response.text}")
            return False
