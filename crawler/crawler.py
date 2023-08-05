import toml
import requests
import logging
from github.github_actor import GithubActor
from .flow_adapter import FlowAdapter
import re
import tools.log_config as log_config

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

        logger.info(f"Fetched {toml_name}")
        return data

    def collect_repos(self, toml_name, include_sub_ecosystem, repos=None, data=None):
        if repos is None:
            repos = set()

        if data is None:
            logger.info(f"[*] Main toml file: {toml_name}")
            data = self.fetch_toml(toml_name)
            repos = repos.union(set([repo["url"] for repo in data["repo"]]))

        for sub_ecosystem in data["sub_ecosystems"] and include_sub_ecosystem:
            logger.info(f"[*] Sub ecosystem: {sub_ecosystem}")
            toml_name = self._transform_ecosystem_name(sub_ecosystem)
            data = self.fetch_toml(toml_name)
            if data["sub_ecosystems"]:
                repos = self.collect_repos(toml_name, repos, data)
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
            repos = repos.union(self.collect_repos(crawler_toml, include_sub_ecosystem, repos=repos))

        logger.info(f"[*] Found {len(repos)} repos for {protocol_name} protocol")

        repo_metadata_list = []
        for repo in repos:
            url_parts = repo.split("/")
            owner = url_parts[-2]
            repo_name = url_parts[-1]

            data = self.actor.check_repo_validity(owner, repo_name)
            if data is False:
                logger.warning(
                    f"[-] {owner}/{repo_name} is not accessible. Will be added to project metadata list, but will not be included in statistics."
                )
                repo_metadata = {
                    "owner": owner,
                    "repo": repo_name,
                    "is_closed": True,
                    "valid": False,
                    "categories.lvl0": [],
                    "url": f"https://github.com/{owner}/{repo_name}"
                }
            else:
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
                    "is_fork": data["fork"],
                    "is_archived": data["archived"],
                    "is_empty": data["size"] == 0,
                    "is_closed": False,
                }
                if repo_metadata["is_empty"] or repo_metadata["is_archived"] or repo_metadata["is_fork"]:
                    repo_metadata["valid"] = False

                else:
                    repo_metadata["valid"] = True

            for adapter in self.adapters:
                adapter.run(repo_metadata)

            repo_metadata_list.append(repo_metadata)

            logger.info(f"[+] Added {owner}/{repo_name} to project metadata list.")

        repo_metadata_list = sorted(repo_metadata_list, key=lambda x: x["stars"], reverse=True)

        for repo_metadata in repo_metadata_list:
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
