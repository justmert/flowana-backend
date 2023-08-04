import firebase_admin
from firebase_admin import firestore, credentials
import os
import argparse
import toml
import requests
import logging
import tools.log_config as log_config
from github.github_actor import GithubActor
from bs4 import BeautifulSoup
from .flow_adapter import FlowAdapter
import re

logger = logging.getLogger(__name__)


class Crawler:
    def __init__(self, app, db, actor: GithubActor):
        self.base_url = "https://raw.githubusercontent.com/electric-capital/crypto-ecosystems/master/data/ecosystems/"
        self.app = app
        self.db = db
        self.actor = actor
        self.adapters = []

    def check_adapters(self, protocol_name):
        # adaptors for custom rules for crawler per protocol
        if protocol_name == "flow":
            # adapters
            flow_adapter = FlowAdapter()
            flow_adapter.init()
            self.adapters.append(flow_adapter)
            logger.info("Flow blockchain adapter initialized.")

    def _transform_ecosystem_name(self, name):
        # Lowercase the name
        name = name.lower()

        # Replace spaces and commas with hyphens
        name = name.replace(" ", "-").replace(",", "-")

        # Replace other special characters
        name = name.replace("@", "-").replace("(", "-").replace(")", "-")

        # Remove leading and trailing hyphens from each part of the name
        name_parts = name.split("-")
        name_parts = [part.strip("-") for part in name_parts]

        # Rejoin the parts of the name
        name = "-".join(name_parts)

        # Remove any remaining disallowed characters (GitHub repo names only allow alphanumeric and hyphens)
        name = re.sub(r"[^a-z0-9\-]", "", name)

        # Replace consecutive hyphens with a single hyphen
        name = re.sub(r"-+", "-", name)

        # Add .toml at the end
        name += ".toml"

        return name

    def fetch_toml(self, toml_name):
        toml_url = self.base_url + f"{toml_name[0:1]}/{toml_name}"

        # Download the file from the URL
        response = requests.get(toml_url)
        content = response.content.decode("utf-8")

        # Parse the TOML file
        data = toml.loads(content)

        logger.info(f"Fetched {toml_name}.toml")
        return data

    def collect_repos(self, toml_name, repos, data=None):
        if data is None:
            logger.info(f"[*] Main toml file: {toml_name}.toml")
            data = self.fetch_toml(toml_name)

        for sub_ecosystem in data["sub_ecosystems"]:
            logger.info(f"[*] Sub ecosystem: {sub_ecosystem}")
            toml_name = self._transform_ecosystem_name(sub_ecosystem)
            data = self.fetch_toml(toml_name)
            if data["sub_ecosystems"]:
                self.collect_repos(toml_name, repos, data)
            else:
                repos = repos.union(set([repo["url"] for repo in data["repo"]]))
        return repos

    def run(self, protocol_name, crawler_config):
        logger.info(f"[*] Running crawler for {protocol_name} protocol")
        self.check_adapters(protocol_name)

        include_sub_ecosystem = crawler_config["include_sub_ecosystems"]
        crawler_tomls = set([toml_name.lower() for toml_name in crawler_config["tomls"]])

        repos = set()
        for crawler_toml in crawler_tomls:
            if include_sub_ecosystem:
                logger.info(f"[!] Including sub ecosystems for protocol {protocol_name}")
                repos = repos.union(self.collect_repos(crawler_toml, repos))
            else:
                logger.info(f"[!] NOT including sub ecosystems for protocol {protocol_name}")
                repos = repos.union(set([repo["url"] for repo in self.fetch_toml(crawler_toml)["repo"]]))

        logger.info(f"[*] Found {len(repos)} repos for {protocol_name} protocol")

        repo_metadata_list = []
        for repo in repos:
            url_parts = repo.split("/")
            owner = url_parts[-2]
            repo_name = url_parts[-1]

            repo_accessible = self.actor.check_repo_validity(owner, repo_name)
            if repo_accessible is False:
                logger.warning(
                    f"[-] {owner}/{repo_name} is not accessible. Will not be added to project metadata list."
                )
                continue

            data = self.actor.github_rest_make_request(f"/repos/{owner}/{repo_name}", max_page_fetch=1)
            repo_metadata = {
                "owner": owner,
                "repo": repo_name,
                "description": data["description"],
                "categories.lvl0": [] if data["topics"] is None else data["topics"],
                "url": data["html_url"],
                "stars": data["stargazers_count"],
                "avatar_url": data["owner"]["avatar_url"],
                "created_at": data["created_at"],
                "updated_at": data["updated_at"],
            }

            for adapter in self.adapters:
                adapter.run(repo_metadata)

            self.repo_metadata_list.append(repo_metadata)

            logger.info(f"[+] Added {owner}/{repo_name} to project metadata list.")

        self.repo_metadata_list = sorted(self.repo_metadata_list, key=lambda x: x["stars"], reverse=True)

        for repo_metadata in self.repo_metadata_list:
            self.db.collection(f"{protocol_name}-projects").document(
                f"{repo_metadata['owner']}#{repo_metadata['repo']}"
            ).set(
                repo_metadata,
                merge=True,
            )
            logger.info(f"[./] Added {repo_metadata['owner']}/{repo_metadata['repo']} to Firestore Database.")

        logger.info(f"[*] Crawler for {protocol_name} protocol finished.")


if __name__ == "__main__":
    pass
