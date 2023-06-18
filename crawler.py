import firebase_admin
from firebase_admin import firestore, credentials
import os
import argparse
import toml
import requests
import logging
import log_config
from request_actor import Actor

logger = logging.getLogger(__name__)


class Crawler:
    def __init__(self, app, db, actor: Actor):
        self.base_url = 'https://raw.githubusercontent.com/electric-capital/crypto-ecosystems/master/data/ecosystems/'
        self.app = app
        self.db = db
        self.actor = actor

    def run(self, protocol_name, crawler_tomls):
        with self.actor.get_session.cache_disabled():
            for crawler_toml in crawler_tomls:
                logger.info(f"Parsing {crawler_toml}")
                toml_url = self.base_url + \
                    f'{crawler_toml[0:1].lower()}/{crawler_toml.lower()}.toml'

                # Download the file from the URL
                response = requests.get(toml_url)
                content = response.content.decode('utf-8')

                # Parse the TOML file
                data = toml.loads(content)

                # Extract owner and repo name for each repository
                repos = data['repo']
                for repo in repos:
                    url_parts = repo['url'].split('/')
                    owner = url_parts[-2]
                    repo_name = url_parts[-1]

                    repo_accessible = self.actor.check_repo_validity(
                        owner, repo_name)
                    if repo_accessible is False:
                        logger.info(
                            f"[-] {owner}/{repo_name} is not accessible. Skipping...")
                        continue

                    data = self.actor.github_rest_make_request(
                        f"/repos/{owner}/{repo_name}", max_page_fetch=1)

                    self.db.collection(f"{protocol_name}-projects").document(f"{owner}#{repo_name}").set(
                        {
                            "owner": owner,
                            "repo": repo_name,
                            "description": data['description'],
                            "topics": data['topics']
                        }
                    )
                    logger.info(
                        f"[+] Added {owner}/{repo_name} to {protocol_name}-projects")
                logger.info(f"Finished parsing {crawler_toml}")


if __name__ == '__main__':
    pass
