from .github_actor import GithubActor
import logging
import tools.log_config as log_config

logger = logging.getLogger(__name__)
import datetime

class GithubLeaderboard:
    def __init__(self, actor: GithubActor, collection_refs):
        self.actor = actor
        self.collection_refs = collection_refs

    def is_valid(self, response):
        if response is None:
            return False

        elif isinstance(response, dict) and not response:
            return False

        elif isinstance(response, list) and not response:
            return False

        return True

    def project_leaderboard(self, **kwargs):
        reference_list = []

        repo_doc = self.collection_refs["widgets"].document("repositories")
        for subcollection in repo_doc.collections():
            try:
                health_score_doc_val = (
                    subcollection.document("health_score").get(field_paths=["data"]).to_dict().get("data")
                )
                repository_info_doc_val = (
                    subcollection.document("repository_info").get(field_paths=["data"]).to_dict().get("data")
                )
            except Exception as ex:
                logger.warning(f"Leaderboard project data fetch error: {ex}")
                continue

            reference_list.append({"repository_info": repository_info_doc_val, "health_score": health_score_doc_val})

        reference_list = sorted(reference_list, key=lambda k: k["health_score"]["total"], reverse=True)[:15]

        self.collection_refs["leaderboard"].document("project_leaderboard").set({"data": reference_list})

    def contributor_leaderboard(self, **kwargs):
        # Initialize a dictionary to store total commits per contributor
        total_contributors = {}

        repo_doc = self.collection_refs["widgets"].document("repositories")
        for subcollection in repo_doc.collections():
            owner, repo = subcollection.id.split("#")
            # Initialize an empty list to hold all contributors
            project_contributor_data = []

            # Fetch all documents that start with 'contributors' in their name
            all_docs = subcollection.list_documents()
            for doc in all_docs:
                if "contributors" in doc.id:
                    # Get 'data' field from each 'contributors' document and extend the all_contributors list
                    doc_dict = doc.get().to_dict()
                    if doc_dict and "data" in doc_dict:
                        project_contributor_data.extend(doc_dict["data"])

            # Iterate over each contributor and add their total to the dictionary
            for contributor in project_contributor_data:
                if contributor["author"]["login"] in total_contributors:
                    contribution_does_exist = total_contributors[contributor["author"]["login"]]["contributions"].get(
                        subcollection.id
                    )
                    if contribution_does_exist:
                        total_contributors[contributor["author"]["login"]]["contributions"][subcollection.id][
                            "commits"
                        ] += contributor["total"]
                        total_contributors[contributor["author"]["login"]]["total_commits"] += contributor["total"]
                    else:
                        total_contributors[contributor["author"]["login"]]["contributions"][subcollection.id] = {
                            "owner": owner,
                            "repo": repo,
                            "html_url": f"https://github.com/{owner}/{repo}",
                        }
                        total_contributors[contributor["author"]["login"]]["total_commits"] += contributor["total"]

                else:
                    total_contributors[contributor["author"]["login"]] = {
                        "author": {
                            "login": contributor["author"]["login"],
                            "avatar_url": contributor["author"]["avatar_url"],
                            "html_url": contributor["author"]["html_url"],
                        },
                        "contributions": {
                            subcollection.id: {
                                "owner": owner,
                                "repo": repo,
                                "html_url": f"https://github.com/{owner}/{repo}",
                                "commits": contributor["total"],
                            }
                        },
                        "total_commits": contributor["total"],
                    }

        # Sort the contributors by total commits and take the top 15
        leaderboard = sorted(total_contributors.values(), key=lambda x: x["total_commits"], reverse=True)[:15]

        self.collection_refs["leaderboard"].document("contributor_leaderboard").set({"data": leaderboard})

        # Return the leaderboard
        return leaderboard


    def write_last_updated(self, **kwargs):
        # datetime in rfc3339 format
        rfc_format = datetime.now().isoformat() + "Z"
        self.collection_refs["leaderboard"].document(f"last_updated_at").set({"data": rfc_format})


