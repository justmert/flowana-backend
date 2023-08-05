import requests
import logging
from bs4 import BeautifulSoup
import re
import tools.log_config as log_config

logger = logging.getLogger(__name__)


class FlowAdapter:
    def __init__(self):
        self.hackathon_projects = []

    def init(self):
        res = requests.get("http://tymianek.com/flow/")
        soup = BeautifulSoup(res.text, "html.parser")

        github_links = []

        for link in soup.find_all("a", href=True):
            if re.search("https://github.com", link["href"]):
                github_links.append(link["href"])

        github_links.sort()
        self.hackathon_projects = set(github_links)

    def run(self, repo_metadata):
        if repo_metadata["url"] in self.hackathon_projects:
            repo_metadata["categories.lvl0"].append("hackathon")
            self.hackathon_projects.remove(repo_metadata["url"])
