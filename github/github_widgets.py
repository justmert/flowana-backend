from .github_actor import GithubActor
import logging
from datetime import datetime
from enum import Enum
import pandas as pd
from datetime import timedelta
import math
import numpy as np
from google.cloud import exceptions
from datetime import timezone
from dateutil import parser
import numpy as np
from sklearn.preprocessing import MinMaxScaler
import numpy as np
import math
from datetime import datetime, timezone
from sklearn.preprocessing import RobustScaler, MinMaxScaler

logger = logging.getLogger(__name__)


class GithubWidgets:
    def __init__(self, actor: GithubActor, collection_refs):
        self.actor = actor
        self.collection_refs = collection_refs

    def get_db_ref(self, owner, repo):
        return (
            self.collection_refs["widgets"]
            .document("repositories")
            .collection(self.get_repo_hash(owner, repo))
        )

    def is_valid(self, response):
        if response is None:
            return False

        elif isinstance(response, dict) and not response:
            return False

        elif isinstance(response, list) and not response:
            return False

        return True

    def get_repo_hash(self, owner, repo):
        return "#".join([owner, repo])

    def repository_info(self, owner: str, repo: str, **kwargs):
        query = """
            query ($owner: String!, $name: String!) {
                repository(owner: $owner, name: $name) {
                    defaultBranchRef {
                        target {
                            ... on Commit {
                                history {
                                    totalCount
                                }
                            }
                        }
                    }
                    forkCount
                    stargazerCount
                    createdAt
                    updatedAt
                    pullRequests {
                        totalCount
                    }
                    commitComments {
                        totalCount
                    }
                    issues {
                        totalCount
                    }
                    description
                    repositoryTopics(first:100) {
                        nodes {
                            topic {
                                name
                            }
                        }
                    }
                    watchers {
                        totalCount
                    }
                    isFork
                    isArchived
                    isEmpty
                    url
                    owner {
                        login
                        avatarUrl
                    }
                    updatedAt
                    releases {
                        totalCount
                    }
                    primaryLanguage {
                        name
                        color
                    }
                    environments {
                        totalCount
                    }
                    diskUsage
                }
            }
        """

        # Flatten the response to a dictionary
        flattened_data = {}

        result = self.actor.github_graphql_make_query(
            query, {"owner": owner, "name": repo}
        )
        repository = result["data"]["repository"]
        if repository is None:
            logger.warning(
                f"[-] {owner}/{repo} is not accessible. Will be added to project metadata list, but will not be included in statistics."
            )
            flattened_data = {
                "owner": owner,
                "repo": repo,
                "is_closed": True,
                "valid": False,
                "categories.lvl0": [],
                "stargazers_count": 0,
                "url": f"https://github.com/{owner}/{repo}",
            }

        else:
            flattened_data["default_branch_commit_count"] = (
                repository["defaultBranchRef"]["target"]["history"]["totalCount"]
                if repository["defaultBranchRef"] is not None
                else None
            )
            flattened_data["fork_count"] = repository["forkCount"]
            flattened_data["stargazers_count"] = repository["stargazerCount"]
            flattened_data["created_at"] = repository["createdAt"]
            flattened_data["updated_at"] = repository["updatedAt"]
            flattened_data["pull_request_count"] = repository["pullRequests"][
                "totalCount"
            ]
            flattened_data["commit_comment_count"] = repository["commitComments"][
                "totalCount"
            ]
            flattened_data["issue_count"] = repository["issues"]["totalCount"]
            flattened_data["description"] = repository["description"]
            flattened_data["categories.lvl0"] = [
                node["topic"]["name"]
                for node in repository["repositoryTopics"]["nodes"]
            ]
            if flattened_data["categories.lvl0"] is None:
                flattened_data["categories.lvl0"] = []
            flattened_data["watcher_count"] = repository["watchers"]["totalCount"]
            flattened_data["is_fork"] = repository["isFork"]
            flattened_data["is_archived"] = repository["isArchived"]
            flattened_data["is_empty"] = repository["isEmpty"]
            flattened_data["url"] = repository["url"]
            flattened_data["owner_login"] = repository["owner"]["login"]
            flattened_data["owner_avatar_url"] = repository["owner"]["avatarUrl"]
            flattened_data["release_count"] = repository["releases"]["totalCount"]
            flattened_data["primary_language_name"] = (
                repository["primaryLanguage"]["name"]
                if repository["primaryLanguage"] is not None
                else None
            )
            flattened_data["primary_language_color"] = (
                repository["primaryLanguage"]["color"]
                if repository["primaryLanguage"] is not None
                else None
            )
            flattened_data["environment_count"] = repository["environments"][
                "totalCount"
            ]
            flattened_data["disk_usage"] = repository["diskUsage"]
            flattened_data["is_closed"] = False
            if (
                flattened_data["is_empty"]
                or flattened_data["is_archived"]
                or flattened_data["is_fork"]
            ):
                flattened_data["valid"] = False
            else:
                flattened_data["valid"] = True

        self.get_db_ref(owner, repo).document("repository_info").set(
            {"data": flattened_data}
        )
        return flattened_data

    def _calculate_time_decayed_scores(self, timestamps, commit_counts, now_timestamp):
        lambda_ = 0.05  # Decay parameter, less aggressive than before
        decayed_scores = []
        for week_timestamp, commit_count in zip(timestamps, commit_counts):
            weeks_ago = (now_timestamp - week_timestamp) // (7 * 24 * 60 * 60)
            decayed_score = commit_count * math.exp(-lambda_ * weeks_ago)
            decayed_scores.append(decayed_score)
        return decayed_scores

    def _calculate_consistency(self, timestamps, now_timestamp):
        # Calculate intervals between commits in weeks
        intervals = np.diff(sorted(timestamps)) / (7 * 24 * 60 * 60)
        if len(intervals) > 1:
            std_dev_intervals = np.std(intervals)
            # Normalize standard deviation using MinMaxScaler
            scaler = MinMaxScaler()
            normalized_std_dev = scaler.fit_transform(
                np.array([std_dev_intervals]).reshape(-1, 1)
            ).flatten()[0]
            # Invert the normalized std dev so that higher values indicate better consistency
            consistency_score = 1 - normalized_std_dev
        else:
            # If there's only one interval, default to perfect consistency
            consistency_score = 1.0
        return consistency_score

    def _score_commit_activity(self, owner, repo):
        try:
            ref = (
                self.get_db_ref(owner, repo)
                .document("commit_activity")
                .get(field_paths=["data"])
                .to_dict()
            )
            default_branch_commit_count = 0
            try:
                ref2 = (
                    self.get_db_ref(owner, repo)
                    .document("repository_info")
                    .get(field_paths=["data"])
                    .to_dict()
                )
            except exceptions.NotFound:
                # Handle case where document or collection does not exist
                pass
            else:
                repository_info = ref2.get("data", None)
                if repository_info is not None:
                    default_branch_commit_count = repository_info.get(
                        "default_branch_commit_count", 0
                    )
            if ref is None:
                raise exceptions.NotFound("Collection or document not found")
        except exceptions.NotFound:
            # Handle case where document or collection does not exist
            pass
        else:
            commit_activity = ref.get("data", None)
            if commit_activity:
                # Current date
                now = datetime.now(timezone.utc)
                now_timestamp = int(now.timestamp())

                # Extract commit counts and timestamps
                commit_counts = [data["total"] for data in commit_activity]
                commit_timestamps = [data["week"] for data in commit_activity]

                # Normalizing commit counts using RobustScaler to handle outliers
                scaler = RobustScaler()
                normalized_commit_counts = scaler.fit_transform(
                    np.array(commit_counts).reshape(-1, 1)
                ).flatten()

                # Calculate time decayed activity scores
                decayed_scores = self._calculate_time_decayed_scores(
                    commit_timestamps, normalized_commit_counts, now_timestamp
                )

                # Calculate consistency measure
                consistency_score = self._calculate_consistency(
                    commit_timestamps, now_timestamp
                )

                # Calculate a baseline score from the total commit count
                baseline_score = np.log1p(default_branch_commit_count)

                # Combine the baseline score, decayed scores, and consistency
                commit_activity_score = baseline_score + np.sum(decayed_scores) * (
                    1 - consistency_score
                )
                print(owner, repo, commit_activity_score)
                return commit_activity_score
        return 0

    def _score_pull_request_activity(self, owner, repo):
        try:
            # Initialize the base reference
            doc_base = self.get_db_ref(owner, repo)

            # Initialize an empty list to hold all contributors
            pull_requests = []

            # Fetch all documents that start with 'contributors' in their name
            all_docs = doc_base.list_documents()
            for doc in all_docs:
                if "pull_request_activity" in doc.id:
                    # Get 'data' field from each 'contributors' document and extend the all_contributors list
                    doc_dict = doc.get().to_dict()
                    if doc_dict and "data" in doc_dict:
                        pull_requests.extend(doc_dict["data"])

        except exceptions.NotFound:
            # Handle case where document or collection does not exist
            pass

        else:
            pull_request_scores = []
            if pull_requests:
                # Weights for open and closed issues
                weight_closed = 1.25
                weight_open = 1

                # Decay parameter
                lambda_ = 0.005

                # Comment reward factor
                comment_reward_factor = 0.05  # for example

                # Current date
                now = datetime.now(timezone.utc)

                # Time-Weighted Pull request Activity Score
                PRAS = 0
                for pull_request in pull_requests:
                    days_since_created = (
                        now - parser.isoparse(pull_request["createdAt"])
                    ).days
                    comment_reward = (
                        pull_request["comment_count"] * comment_reward_factor
                    )
                    if pull_request["closed"]:
                        days_since_closed = (
                            now - parser.isoparse(pull_request["closedAt"])
                        ).days
                        PRAS += (weight_closed + comment_reward) * math.exp(
                            -lambda_ * days_since_closed
                        )
                    else:  # issue is still open
                        PRAS += (weight_open + comment_reward) * math.exp(
                            -lambda_ * days_since_created
                        )
                    pull_request_scores.append(PRAS)

                return sum(pull_request_scores)
        return 0

    def _score_issue_activity(self, owner, repo):
        try:
            # Initialize the base reference
            doc_base = self.get_db_ref(owner, repo)

            # Initialize an empty list to hold all contributors
            issues = []

            # Fetch all documents that start with 'contributors' in their name
            all_docs = doc_base.list_documents()
            for doc in all_docs:
                if "issue_activity" in doc.id:
                    # Get 'data' field from each 'contributors' document and extend the all_contributors list
                    doc_dict = doc.get().to_dict()
                    if doc_dict and "data" in doc_dict:
                        issues.extend(doc_dict["data"])

        except exceptions.NotFound:
            # Handle case where document or collection does not exist
            pass

        else:
            issue_scores = []
            if issues:
                # Weights for open and closed issues
                weight_closed = 1.25
                weight_open = 1

                # Decay parameter
                lambda_ = 0.005

                # Average time to close an issue (in days)
                # avg_days_to_close_issue = 7

                # Comment reward factor
                comment_reward_factor = 0.05  # for example

                # Current date
                now = datetime.now(timezone.utc)

                # Time-Weighted Issue Activity Score
                for issue in issues:
                    days_since_created = (
                        now - parser.isoparse(issue["createdAt"])
                    ).days
                    comment_reward = issue["comment_count"] * comment_reward_factor
                    issue_score = 0
                    if issue["closed"]:
                        days_since_closed = (
                            now - parser.isoparse(issue["closedAt"])
                        ).days
                        issue_score = (weight_closed + comment_reward) * math.exp(
                            -lambda_ * days_since_closed
                        )
                    else:  # issue is still open
                        issue_score = (weight_open + comment_reward) * math.exp(
                            -lambda_ * days_since_created
                        )
                    # Append individual issue score to the list
                    issue_scores.append(issue_score)

                return sum(issue_scores)
        return 0

    def _score_release_activity(self, owner, repo):
        try:
            ref = (
                self.get_db_ref(owner, repo)
                .document("recent_releases")
                .get(field_paths=["data"])
                .to_dict()
            )

            if ref is None:
                raise exceptions.NotFound("Collection or document not found")

        except exceptions.NotFound:
            # Handle case where document or collection does not exist
            pass

        else:
            releases = ref.get("data", None)
            if releases:
                # Convert 'published_at' strings to datetime objects and sort in descending order
                release_dates = sorted(
                    [parser.isoparse(release["published_at"]) for release in releases],
                    reverse=True,
                )

                # Decay parameter
                lambda_ = 0.1

                # Penalty parameter
                penalty_param = 0.01

                # Compute the list of release intervals (in days)
                release_intervals = [
                    (release_dates[i - 1] - release_dates[i]).days
                    for i in range(1, len(release_dates))
                ]

                # Compute the standard deviation of the release intervals
                if len(release_dates) == 1:
                    std_dev = 0  # or some other appropriate default value
                    time_since_last_release = (
                        datetime.now(timezone.utc) - release_dates[0]
                    ).days
                    # Adjust the penalty and score computation if necessary
                    penalty = 1 / (1 + penalty_param * time_since_last_release)
                    RAS = penalty  # Adjust this as needed
                else:
                    std_dev = np.std(release_intervals)
                    inverse_std_dev = 1 / (std_dev + 0.01)
                    time_since_last_release = (
                        datetime.now(timezone.utc) - release_dates[0]
                    ).days
                    penalty = 1 / (1 + penalty_param * time_since_last_release)
                    RAS = 0
                    for i in range(len(release_dates)):
                        RAS += penalty * math.exp(-lambda_ * i) * inverse_std_dev

                return RAS
        return 0

    def _score_contributors_data(self, owner, repo, **kwargs):
        def calculate_EMA(data, alpha=0.1):
            EMA = [data[0]]  # initialize with the first data point
            for i in range(1, len(data)):
                EMA.append(alpha * data[i] + (1 - alpha) * EMA[i - 1])
            return EMA

        try:
            # Initialize the base reference
            doc_base = self.get_db_ref(owner, repo)

            # Initialize an empty list to hold all contributors
            data = []

            # Fetch all documents that start with 'contributors' in their name
            all_docs = doc_base.list_documents()

            for doc in all_docs:
                if "contributors" in doc.id:
                    # Get 'data' field from each 'contributors' document and extend the all_contributors list
                    doc_dict = doc.get().to_dict()
                    if doc_dict and "data" in doc_dict:
                        data.extend(doc_dict["data"])

        except exceptions.NotFound:
            # Handle case where document or collection does not exist
            pass

        else:
            if data:
                # Aggregate average additions, deletions, and commits from all contributors
                additions, deletions, commits, commit_weeks = [], [], [], []
                for contributor in data:
                    weeks = np.array([week["w"] for week in contributor["weeks"]])
                    num_weeks = len(weeks)
                    # Now giving higher weight to recent weeks
                    decay_weights = np.array(
                        [i / num_weeks for i in range(1, num_weeks + 1)]
                    )

                    weighted_additions = (
                        np.array([week["a"] for week in contributor["weeks"]])
                        * decay_weights
                    )
                    weighted_deletions = (
                        np.array([week["d"] for week in contributor["weeks"]])
                        * decay_weights
                    )
                    weighted_commits = (
                        np.array([week["c"] for week in contributor["weeks"]])
                        * decay_weights
                    )

                    additions.extend(weighted_additions)
                    deletions.extend(weighted_deletions)
                    commits.extend(weighted_commits)
                    commit_weeks.extend(weeks)

                # Normalize each score to be between 0 and 1
                scaler = MinMaxScaler()
                normalized_additions = scaler.fit_transform(
                    np.array(additions).reshape(-1, 1)
                )
                normalized_deletions = scaler.fit_transform(
                    np.array(deletions).reshape(-1, 1)
                )
                normalized_commits = scaler.fit_transform(
                    np.array(commits).reshape(-1, 1)
                )

                # Calculate commit trend
                EMA_commits = calculate_EMA(
                    normalized_commits.flatten(), alpha=0.1
                )  # calculate EMA
                # the trend can be the difference between the last and the first EMA
                commit_trend = EMA_commits[-1] - EMA_commits[0]
                commit_trend = (commit_trend + 1) / 2

                # Compute the new metric
                additions_deletions_ratio = np.divide(
                    normalized_additions,
                    normalized_deletions,
                    out=np.zeros_like(normalized_additions),
                    where=normalized_deletions != 0,
                )
                new_metric = additions_deletions_ratio * normalized_commits
                new_metric_average = np.mean(new_metric)

                # Calculate Gini coefficient for contributor equality
                contributors = np.array([contributor["total"] for contributor in data])
                contributors.sort()

                if len(contributors) == 1:
                    contributor_gini_coefficient = 1
                else:
                    n = len(contributors)
                    contributor_gini_coefficient = (
                        2 * np.sum((np.arange(1, n + 1) * np.sort(contributors)))
                    ) / (n * np.sum(contributors)) - (n + 1) / n

                MAX_CONTRIBUTORS = 100
                contributor_count = len(data) / MAX_CONTRIBUTORS

                # Define the weights
                weight_commit_trend = 0.20  # assign 25% importance to commit trend
                # assign 25% importance to contributor gini coefficient
                weight_contributor_gini_coefficient = 0.30
                weight_new_metric_average = (
                    0.20  # assign 25% importance to new metric average
                )
                weight_contributor_count = (
                    0.30  # assign 25% importance to contributor count
                )

                # Calculate weighted sum
                liveness_score = (
                    weight_commit_trend * commit_trend
                    + weight_contributor_gini_coefficient
                    * (1 - contributor_gini_coefficient)
                    + weight_new_metric_average * new_metric_average
                    + weight_contributor_count * contributor_count
                )
                return liveness_score

        return 0

    def health_score(self, owner, repo, **kwargs):
        raw_health_score = {
            "commit_activity": self._score_commit_activity(owner, repo, **kwargs),
            "issue_activity": self._score_issue_activity(owner, repo, **kwargs),
            "pull_request_activity": self._score_pull_request_activity(
                owner, repo, **kwargs
            ),
            "release_activity": self._score_release_activity(owner, repo, **kwargs),
            "contribution_activity": self._score_contributors_data(
                owner, repo, **kwargs
            ),
        }

        self.get_db_ref(owner, repo).document("raw_health_score").set(
            {"data": raw_health_score}
        )

    def commit_activity(self, owner, repo, **kwargs):
        # formatting will be in frontend
        data = self.actor.github_rest_make_request(
            f"/repos/{owner}/{repo}/stats/commit_activity"
        )

        if not self.is_valid(data) or not any(item["total"] > 0 for item in data):
            logger.warning("[!] Invalid or empty data returned")
            return

        self.get_db_ref(owner, repo).document("commit_activity").set({"data": data})

    def contributors(self, owner, repo, **kwargs):
        # formatting will be in frontend
        data = self.actor.github_rest_make_request(
            f"/repos/{owner}/{repo}/stats/contributors"
        )
        sorted_data = sorted(data, key=lambda x: x["total"], reverse=True)

        if not self.is_valid(data):
            logger.warning("[!] Invalid or empty data returned")
            return

        # Chunk size
        chunk_size = 20
        # Splitting the data into chunks of size `chunk_size`
        for i in range(0, len(sorted_data), chunk_size):
            chunk = sorted_data[i : i + chunk_size]

            # Construct the document name ('contributors1', 'contributors2', ...)
            doc_name = f"contributors{i // chunk_size + 1}"

            logger.info(f"[#db] Writing to database {owner}/{repo}/{doc_name}")

            self.get_db_ref(owner, repo).document(doc_name).set({"data": chunk})

    def participation(self, owner, repo, **kwargs):
        # formatting will be in here
        data = self.actor.github_rest_make_request(
            f"/repos/{owner}/{repo}/stats/participation"
        )

        if not self.is_valid(data) or not sum(data["all"]) > 0:
            logger.warning("[!] Invalid or empty data returned")
            return

        # Generate dates for last 52 weeks
        today_utc = datetime.utcnow()  # Switch from datetime.now() to datetime.utcnow()
        dates = [
            (today_utc - timedelta(weeks=i)).strftime("%Y-%m-%d") for i in range(52)
        ][::-1]

        chart_data = {
            "xAxis": {"type": "category", "data": dates},
            "yAxis": {"type": "value"},
            "series": [],
        }

        # Subtract 'owner' from 'all' to get 'others'
        others = [
            all_count - owner_count
            for all_count, owner_count in zip(data["all"], data["owner"])
        ]

        owner_sum = sum(data["owner"])
        others_sum = sum(others)

        chart_data["series"] = [
            {"name": "All", "data": data["all"], "type": "line"},
            {"name": "Owners", "data": data["owner"], "type": "line"},
            {"name": "Others", "data": others, "type": "line"},
        ]

        self.get_db_ref(owner, repo).document("participation").set({"data": chart_data})

        self.get_db_ref(owner, repo).document("participation_count").set(
            {"data": {"owner": owner_sum, "others": others_sum}}
        )

    def code_frequency(self, owner, repo, **kwargs):
        # formatting will be in here
        data = self.actor.github_rest_make_request(
            f"/repos/{owner}/{repo}/stats/code_frequency"
        )

        if not self.is_valid(data) and not any(item[1] or item[2] for item in data):
            logger.warning("[!] Invalid or empty data returned")
            return

        # Define the chart option
        chart_data = {
            "xAxis": {
                "data": [str(datetime.fromtimestamp(item[0]).date()) for item in data]
            },
            "yAxis": {},
            "series": [
                {"data": [], "type": "line", "stack": "x"},
                {"data": [], "type": "line", "stack": "x"},
            ],
        }
        chart_data["series"][0]["data"] = [item[1] for item in data]
        chart_data["series"][1]["data"] = [item[2] for item in data]

        self.get_db_ref(owner, repo).document("code_frequency").set(
            {"data": chart_data}
        )

    def community_profile(self, owner, repo, **kwargs):
        # formatting will be in frontend
        data = self.actor.github_rest_make_request(
            f"/repos/{owner}/{repo}/community/profile"
        )

        if not self.is_valid(data):
            logger.warning("[!] Invalid or empty data returned")
            return

        self.get_db_ref(owner, repo).document("community_profile").set({"data": data})

    def punch_card(self, owner, repo, **kwargs):
        # formatting will be in frontend
        data = self.actor.github_rest_make_request(
            f"/repos/{owner}/{repo}/stats/punch_card"
        )

        if not self.is_valid(data) or not any(item[2] for item in data):
            logger.warning("[!] Invalid or empty data returned")
            return

        for i in range(len(data)):
            data[i] = {
                "day": data[i][0],
                "hour": data[i][1],
                "commits": data[i][2],
            }

        self.get_db_ref(owner, repo).document("punch_card").set({"data": data})

    def issue_count(self, owner, repo, **kwargs):
        # formatting will be in api
        query = """
        query ($owner: String!, $name: String!) {
            repository(owner: $owner, name: $name) {
                openedIssues: issues(states: OPEN) {
                totalCount
                }
                closedIssues: issues(states: CLOSED) {
                totalCount
                }
            }
        }
        """
        data = self.actor.github_graphql_make_query(
            query, {"owner": owner, "name": repo}
        )

        if not self.is_valid(data):
            logger.warning("[!] Invalid or empty data returned")
            return

        self.get_db_ref(owner, repo).document("issue_count").set(
            {
                "data": {
                    "open": data["data"]["repository"]["openedIssues"]["totalCount"],
                    "closed": data["data"]["repository"]["closedIssues"]["totalCount"],
                }
            }
        )

    def issue_activity(self, owner, repo, **kwargs):
        # formatting will be in api
        query = """
            query ($owner: String!, $name: String!, $cursor: String, $orderBy: IssueOrderField!) {
                repository(owner: $owner, name: $name) {
                    issues(first: 100, after: $cursor, orderBy: {field: $orderBy, direction: DESC}) {
                        pageInfo {
                            endCursor
                            hasNextPage
                        }
                        totalCount
                        nodes {
                            createdAt
                            closedAt
                            closed
                            comments {
								totalCount
							}

                        }
                    }
                }
            }
        """

        # List to store the data points
        issues = []

        # Execute the GraphQL query with pagination
        has_next_page = True
        cursor = None
        while has_next_page:
            # Send the GraphQL request
            result = self.actor.github_graphql_make_query(
                query,
                {
                    "owner": owner,
                    "name": repo,
                    "cursor": cursor,
                    "orderBy": "CREATED_AT",
                },
            )

            if not self.is_valid(result):
                logger.info("[!] Invalid or empty data returned")
                if issues:
                    break
                else:
                    return

            nodes = result["data"]["repository"]["issues"]["nodes"]
            end_cursor = result["data"]["repository"]["issues"]["pageInfo"]["endCursor"]
            has_next_page = result["data"]["repository"]["issues"]["pageInfo"][
                "hasNextPage"
            ]

            # Extract relevant data points
            for node in nodes:
                created_at = node["createdAt"]
                closed_at = node["closedAt"]
                closed = node["closed"]
                issues.append(
                    {
                        "createdAt": created_at,
                        "closedAt": closed_at,
                        "closed": closed,
                        "closed_interval": str(
                            datetime.strptime(closed_at, "%Y-%m-%dT%H:%M:%SZ")
                            - datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%SZ")
                        )
                        if closed
                        else None,
                        "comment_count": node["comments"]["totalCount"],
                    }
                )

            # Set the next cursor for pagination
            cursor = end_cursor

        # Calculate the average days to close an issue
        total_days = 0
        closed_issue_count = 0

        # Process the issues data
        for issue in issues:
            created_at = issue["createdAt"]
            closed_at = issue["closedAt"]
            closed = issue["closed"]

            created_at_time = datetime.strptime(
                issue["createdAt"], "%Y-%m-%dT%H:%M:%SZ"
            )

            if closed:
                closed_at_time = datetime.strptime(
                    issue["closedAt"], "%Y-%m-%dT%H:%M:%SZ"
                )
                days_to_close = (closed_at_time - created_at_time).days
                total_days += days_to_close
                closed_issue_count += 1

        # Chunk size
        chunk_size = 1000

        # Splitting the data into chunks of size `chunk_size`
        for i in range(0, len(issues), chunk_size):
            chunk = issues[i : i + chunk_size]

            # Construct the document name ('contributors1', 'contributors2', ...)
            doc_name = f"issue_activity{i // chunk_size + 1}"

            logger.info(f"[#db] Writing to database {owner}/{repo}/{doc_name}")

            self.get_db_ref(owner, repo).document(doc_name).set({"data": chunk})

        self.get_db_ref(owner, repo).document("average_days_to_close_issue").set(
            {
                "data": round(total_days / closed_issue_count, 2)
                if closed_issue_count > 0
                else 0
            }
        )

        # process here
        for interval in ["week", "month", "year"]:
            # Process data
            issues_df = pd.DataFrame(issues)
            # check if empty
            if issues_df.empty:
                continue

            # Convert createdAt and closedAt to datetime format
            issues_df["createdAt"] = pd.to_datetime(issues_df["createdAt"])
            issues_df["closedAt"] = pd.to_datetime(issues_df["closedAt"])

            pd_interval = None
            if interval == "week":
                pd_interval = "W"

            elif interval == "month":
                pd_interval = "M"

            elif interval == "year":
                pd_interval = "Y"

            # Group by date and count new and closed issues separately
            new_issues = issues_df.resample(pd_interval, on="createdAt").size()
            # closed_issues = issues_df[issues_df['closed']].resample(
            #     pd_interval, on='closedAt').size()

            # Check if there are any closed pull requests
            if any(issues_df["closed"]):
                closed_issues = (
                    issues_df[issues_df["closed"]]
                    .resample(pd_interval, on="closedAt")
                    .size()
                )
            else:
                closed_issues = pd.Series(index=new_issues.index, data=0)

            # Ensure new_issues and closed_issues have the same index
            all_dates = new_issues.index.union(closed_issues.index)
            new_issues = new_issues.reindex(all_dates, fill_value=0)
            closed_issues = closed_issues.reindex(all_dates, fill_value=0)

            # Convert data to ECharts format
            dates = all_dates.strftime("%Y-%m-%d").tolist()
            echart_data = {
                "xAxis": {"data": dates},
                "series": [
                    {"name": "Opened", "data": new_issues.tolist()},
                    {"name": "Closed", "data": closed_issues.tolist()},
                ],
            }

            self.get_db_ref(owner, repo).document(f"issue_chart_{interval}").set(
                {"data": echart_data}
            )

    def pull_request_activity(self, owner, repo, **kwargs):
        # formatting will be in api
        query = """
            query ($owner: String!, $name: String!, $cursor: String, $orderBy: IssueOrderField!) {
                repository(owner: $owner, name: $name) {
                    pullRequests(first: 100, after: $cursor, orderBy: {field: $orderBy, direction: DESC}) {
                        pageInfo {
                            endCursor
                            hasNextPage
                        }
                        totalCount
                        nodes {
                            createdAt
                            closedAt
                            closed
                            comments {
								totalCount
							}
                        }
                    }
                }
            }
        """

        # List to store the data points
        pull_requests = []

        # Execute the GraphQL query with pagination
        has_next_page = True
        cursor = None
        while has_next_page:
            # Send the GraphQL request
            result = self.actor.github_graphql_make_query(
                query,
                {
                    "owner": owner,
                    "name": repo,
                    "cursor": cursor,
                    "orderBy": "CREATED_AT",
                },
            )

            if not self.is_valid(result):
                logger.info("[!] Invalid or empty data returned")
                if pull_requests:
                    break
                else:
                    return

            nodes = result["data"]["repository"]["pullRequests"]["nodes"]
            end_cursor = result["data"]["repository"]["pullRequests"]["pageInfo"][
                "endCursor"
            ]
            has_next_page = result["data"]["repository"]["pullRequests"]["pageInfo"][
                "hasNextPage"
            ]

            # Extract relevant data points
            for node in nodes:
                created_at = node["createdAt"]
                closed_at = node["closedAt"]
                closed = node["closed"]
                pull_requests.append(
                    {
                        "createdAt": created_at,
                        "closed_interval": str(
                            (
                                datetime.strptime(closed_at, "%Y-%m-%dT%H:%M:%SZ")
                                - datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%SZ")
                            )
                        )
                        if closed
                        else None,
                        "closedAt": closed_at,
                        "closed": closed,
                        "comment_count": node["comments"]["totalCount"],
                    }
                )

            # Set the next cursor for pagination
            cursor = end_cursor

        # Calculate the average days to close an pull request
        total_days = 0
        closed_pull_request_count = 0

        # Process the pull request data
        for pull_request in pull_requests:
            created_at = pull_request["createdAt"]
            closed_at = pull_request["closedAt"]
            closed = pull_request["closed"]

            created_at_time = datetime.strptime(
                pull_request["createdAt"], "%Y-%m-%dT%H:%M:%SZ"
            )

            if closed:
                closed_at_time = datetime.strptime(
                    pull_request["closedAt"], "%Y-%m-%dT%H:%M:%SZ"
                )
                days_to_close = (closed_at_time - created_at_time).days
                total_days += days_to_close
                closed_pull_request_count += 1

        # Chunk size
        chunk_size = 1000

        # Splitting the data into chunks of size `chunk_size`
        for i in range(0, len(pull_requests), chunk_size):
            chunk = pull_requests[i : i + chunk_size]

            # Construct the document name ('contributors1', 'contributors2', ...)
            doc_name = f"pull_request_activity{i // chunk_size + 1}"

            logger.info(f"[#db] Writing to database {owner}/{repo}/{doc_name}")

            self.get_db_ref(owner, repo).document(doc_name).set({"data": chunk})

        self.get_db_ref(owner, repo).document("average_days_to_close_pull_request").set(
            {
                "data": round(total_days / closed_pull_request_count, 2)
                if closed_pull_request_count > 0
                else 0
            }
        )

        # interval_chart
        for interval in ["week", "month", "year"]:
            pull_requests_df = pd.DataFrame(pull_requests)
            if pull_requests_df.empty:
                continue

            # Convert createdAt and closedAt to datetime format
            pull_requests_df["createdAt"] = pd.to_datetime(
                pull_requests_df["createdAt"]
            )
            pull_requests_df["closedAt"] = pd.to_datetime(pull_requests_df["closedAt"])

            pd_interval = None
            if interval == "week":
                pd_interval = "W"

            elif interval == "month":
                pd_interval = "M"

            elif interval == "year":
                pd_interval = "Y"

            # Group by date and count new and closed issues separately
            new_pull_requests = pull_requests_df.resample(
                pd_interval, on="createdAt"
            ).size()
            # Check if there are any closed pull requests
            if any(pull_requests_df["closed"]):
                closed_pull_requests = (
                    pull_requests_df[pull_requests_df["closed"]]
                    .resample(pd_interval, on="closedAt")
                    .size()
                )
            else:
                closed_pull_requests = pd.Series(index=new_pull_requests.index, data=0)

            # Ensure new_issues and closed_issues have the same index
            all_dates = new_pull_requests.index.union(closed_pull_requests.index)
            new_pull_requests = new_pull_requests.reindex(all_dates, fill_value=0)
            closed_pull_requests = closed_pull_requests.reindex(all_dates, fill_value=0)

            # Convert data to ECharts format
            dates = all_dates.strftime("%Y-%m-%d").tolist()
            echart_data = {
                "xAxis": {"data": dates},
                "series": [
                    {"name": "Opened", "data": new_pull_requests.tolist()},
                    {"name": "Closed", "data": closed_pull_requests.tolist()},
                ],
            }

            self.get_db_ref(owner, repo).document(f"pull_request_chart_{interval}").set(
                {"data": echart_data}
            )

    def most_active_issues(self, owner, repo, **kwargs):
        # formatting will be in frontend

        count = kwargs.get("count", 10)
        flattened_data = {}

        for interval in ["day", "week", "month", "year"]:
            flattened_data[interval] = []
            if interval == "day":
                current_time = datetime.now(timezone.utc)
                since = current_time - timedelta(days=1)

            elif interval == "week":
                current_time = datetime.now(timezone.utc)
                since = current_time - timedelta(days=7)

            elif interval == "month":
                current_time = datetime.now(timezone.utc)
                since = current_time - timedelta(days=30)

            elif interval == "year":
                current_time = datetime.now(timezone.utc)
                since = current_time - timedelta(days=365)

            query = """
                query ($owner: String!, $name: String!, $since: DateTime!, $count: Int) {
                repository(owner: $owner, name: $name) {
                    issues(first: $count, orderBy: {field: COMMENTS, direction: DESC}, filterBy: {since: $since}) {
                    nodes {
                        number
                        author {
                        avatarUrl
                        login
                        }
                        title
                        state
                        repository {
							name
							owner {
								login
							}
						}
                        url
                        comments {
                        totalCount
                        }
                        createdAt
                        updatedAt
                        closed
						closedAt

                    }
                    }
                }
            }
            """
            data = self.actor.github_graphql_make_query(
                query,
                {
                    "owner": owner,
                    "name": repo,
                    "since": since.isoformat(),
                    "count": count,
                },
            )

            if not self.is_valid(data):
                logger.info(
                    f"[#invalid] No most active issues for repository {owner}/{repo}"
                )
                continue

            # Extract the issues from the response
            issues = data["data"]["repository"]["issues"]["nodes"]

            # Iterate through the issues and flatten the data
            for issue in issues:
                flattened_issue = {
                    "number": issue["number"],
                    "author_avatar_url": None
                    if issue.get("author", {}).get("avatarUrl", None) is None
                    else issue["author"]["avatarUrl"],
                    "author_login": None
                    if issue.get("author", {}).get("login", None) is None
                    else issue["author"]["login"],
                    "title": issue["title"],
                    "state": issue["state"],
                    "comments_count": issue["comments"]["totalCount"],
                    "closed": issue["closed"],
                    "closed_at": issue["closedAt"],
                    "created_at": issue["createdAt"],
                    "updated_at": issue["updatedAt"],
                    "url": issue["url"],
                    "repo": issue["repository"]["name"],
                    "owner": issue["repository"]["owner"]["login"],
                }
                flattened_data[interval].append(flattened_issue)

        self.get_db_ref(owner, repo).document("most_active_issues").set(
            {"data": flattened_data}
        )

    def pull_request_count(self, owner, repo, **kwargs):
        # formatting will be in api
        query = """
        query ($owner: String!, $name: String!) {
            repository(owner: $owner, name: $name) {
                openedPullRequests: pullRequests(states: OPEN) {
                totalCount
                }
                closedPullRequests: pullRequests(states: CLOSED) {
                totalCount
                }
            }
        }
        """
        data = self.actor.github_graphql_make_query(
            query, {"owner": owner, "name": repo}
        )

        if not self.is_valid(data):
            logger.warning("[!] Invalid or empty data returned")
            return

        self.get_db_ref(owner, repo).document("pull_request_count").set(
            {
                "data": {
                    "open": data["data"]["repository"]["openedPullRequests"][
                        "totalCount"
                    ],
                    "closed": data["data"]["repository"]["closedPullRequests"][
                        "totalCount"
                    ],
                }
            }
        )

    def language_breakdown(self, owner, repo, **kwargs):
        # formatting will be in api
        query = """
        query ($owner: String!, $name: String!) {
            repository(owner: $owner, name: $name) {
                languages(first: 100) {
                    edges {
                        size
                        node {
                            name
                        }
                    }
                }
            }
        }
        """
        data = self.actor.github_graphql_make_query(
            query, {"owner": owner, "name": repo}
        )

        if not self.is_valid(data):
            logger.warning("[!] Invalid or empty data returned")
            return

        flattened_data = []

        # Extract the languages from the response
        languages = data["data"]["repository"]["languages"]["edges"]

        # Calculate the total size of the codebase
        total_size = sum(language["size"] for language in languages)

        # Iterate through the languages and calculate the percentage of code written in each language
        for language in languages:
            percentage = round(language["size"] / total_size * 100, 2)
            flattened_language = {
                "name": language["node"]["name"],
                "percentage": percentage,
                "size": language["size"],
            }
            flattened_data.append(flattened_language)

        self.get_db_ref(owner, repo).document("language_breakdown").set(
            {"data": flattened_data}
        )

    class RecentIssuesOrder(Enum):
        CREATED_AT = "CREATED_AT"
        UPDATED_AT = "UPDATED_AT"

    class RecentPullRequestsOrder(Enum):
        CREATED_AT = "CREATED_AT"
        UPDATED_AT = "UPDATED_AT"

    def recent_issues(self, owner, repo, **kwargs):
        # formatting will be in frontend

        order_by = kwargs.get("order_by", self.RecentIssuesOrder.CREATED_AT)

        query = """
        query ($owner: String!, $name: String!, $order_by: IssueOrderField!) {
            repository(owner: $owner, name: $name) {
                issues(first: 10, orderBy: {field: $order_by, direction: DESC}) {
                    nodes {
                        number
                        author {
                            avatarUrl
                            login
                        }
                        title
                        state
                        comments {
                            totalCount
                        }
                        url
                        createdAt
                        updatedAt
                        repository {
							name
							owner {
								login
							}
						}
                    }
                }
            }
        }
        """
        data = self.actor.github_graphql_make_query(
            query, {"owner": owner, "name": repo, "order_by": order_by.value}
        )

        if not self.is_valid(data):
            logger.warning("[!] Invalid or empty data returned")
            return

        flattened_data = []

        # Extract the issues from the response
        issues = data["data"]["repository"]["issues"]["nodes"]

        # Iterate through the issues and flatten the data
        for issue in issues:
            flattened_issue = {
                "number": issue["number"],
                "author_avatar_url": None
                if issue["author"] is None
                else issue["author"].get("avatarUrl", None),
                "author_login": None
                if issue["author"] is None
                else issue["author"].get("login", None),
                "title": issue["title"],
                "state": issue["state"],
                "comments_count": issue["comments"]["totalCount"],
                "created_at": issue["createdAt"],
                "updated_at": issue["updatedAt"],
                "url": issue["url"],
                "repo": issue["repository"]["name"],
                "owner": issue["repository"]["owner"]["login"],
            }
            flattened_data.append(flattened_issue)

        if order_by == self.RecentIssuesOrder.CREATED_AT:
            self.get_db_ref(owner, repo).document("recent_created_issues").set(
                {"data": flattened_data}
            )

        elif order_by == self.RecentIssuesOrder.UPDATED_AT:
            self.get_db_ref(owner, repo).document("recent_updated_issues").set(
                {"data": flattened_data}
            )

    def recent_pull_requests(self, owner, repo, **kwargs):
        # formatting will be in frontend

        order_by = kwargs.get("order_by", self.RecentPullRequestsOrder.CREATED_AT)
        query = f"""
            query ($owner: String!, $name: String!) {{
                repository(owner: $owner, name: $name) {{
                    pullRequests(first: 10, orderBy: {{field: {order_by.value}, direction: DESC}}) {{
                        nodes {{
                            number
                            author {{
                                avatarUrl
                                login
                            }}
                            title
                            state
                            comments {{
                                totalCount
                            }}
                            createdAt
                            updatedAt
                            url
                            repository {{
							name
							owner {{
								login
							}}
						}}

                        }}
                    }}
                }}
            }}
        """

        data = self.actor.github_graphql_make_query(
            query, {"owner": owner, "name": repo, "order_by": order_by.value}
        )

        if not self.is_valid(data):
            logger.warning("[!] Invalid or empty data returned")
            return

        flattened_data = []

        # Extract the pull requests from the response
        pull_requests = data["data"]["repository"]["pullRequests"]["nodes"]

        # Iterate through the pull requests and flatten the data
        for pull_request in pull_requests:
            flattened_pull_request = {
                "number": pull_request["number"],
                "author_avatar_url": None
                if pull_request["author"] is None
                else pull_request["author"].get("avatarUrl", None),
                "author_login": None
                if pull_request["author"] is None
                else pull_request["author"].get("login", None),
                "title": pull_request["title"],
                "state": pull_request["state"],
                "comments_count": pull_request["comments"]["totalCount"],
                "created_at": pull_request["createdAt"],
                "updated_at": pull_request["updatedAt"],
                "url": pull_request["url"],
                "repo": pull_request["repository"]["name"],
                "owner": pull_request["repository"]["owner"]["login"],
            }
            flattened_data.append(flattened_pull_request)

        if order_by == self.RecentPullRequestsOrder.CREATED_AT:
            self.get_db_ref(owner, repo).document("recent_created_pull_requests").set(
                {"data": flattened_data}
            )

        elif order_by == self.RecentPullRequestsOrder.UPDATED_AT:
            self.get_db_ref(owner, repo).document("recent_updated_pull_requests").set(
                {"data": flattened_data}
            )

    def recent_stargazing_activity(self, owner, repo, **kwargs):
        # formatting will be in here

        max_fetch_pages = kwargs.get("max_fetch_pages", 15)
        query = """
            query ($owner: String!, $name: String!, $cursor: String) {
            repository(owner: $owner, name: $name) {
                stargazers(first: 100, orderBy: {field: STARRED_AT, direction: DESC}, after: $cursor) {
                edges {
                    starredAt
                    cursor
                }
                pageInfo {
                    hasNextPage
                }
                }
            }
        }
        """

        cursor = None
        has_next_page = True
        current_page = 0
        star_dict = {}
        while has_next_page and current_page < max_fetch_pages:
            # Make the request
            result = self.actor.github_graphql_make_query(
                query, {"owner": owner, "name": repo, "cursor": cursor}
            )

            if not self.is_valid(result):
                logger.warning("[!] Invalid or empty data returned")
                break

            # Extract the stargazers from the response
            stargazers = result["data"]["repository"]["stargazers"]["edges"]

            # Iterate through the stargazers and flatten the data
            for stargazer in stargazers:
                starred_at = stargazer["starredAt"]
                cursor = stargazer["cursor"]
                if starred_at in star_dict:
                    star_dict[starred_at] += 1
                else:
                    star_dict[starred_at] = 1

            # Check if there is another page of data to fetch
            has_next_page = result["data"]["repository"]["stargazers"]["pageInfo"][
                "hasNextPage"
            ]
            current_page += 1

        if len(star_dict) < 0:
            logger.warning("[!] Invalid or empty data returned")
            return

        # Convert the dictionary to a Pandas DataFrame
        df = pd.DataFrame.from_dict(star_dict, orient="index", columns=["count"])
        df.index = pd.to_datetime(df.index)

        # Group the data by weeks or days depending on the range of dates
        date_range = df.index.max() - df.index.min()
        if date_range > timedelta(days=6 * 30):
            df = df.resample("M").sum()

        elif date_range > timedelta(days=30):
            df = df.resample("W").sum()

        else:
            df = df.resample("D").sum()

        # Convert the datetime index to a string
        df.index = df.index.strftime("%Y-%m-%d")

        # Convert the DataFrame back to a dictionary
        flattened_data = df.to_dict()["count"]

        chart_data = {
            "xAxis": {"data": list(flattened_data.keys())},
            "yAxis": {},
            "series": [
                {
                    "data": list(flattened_data.values()),
                    "type": "line",
                    "stack": "x",
                },
            ],
        }

        self.get_db_ref(owner, repo).document("recent_stargazing_activity").set(
            {"data": chart_data}
        )

    def recent_commits(self, owner, repo, **kwargs):
        # formatting will be in frontend
        query = """
        query ($owner: String!, $name: String!, $count: Int!) {
            repository(owner: $owner, name: $name) {
                defaultBranchRef {
                    target {
                        ... on Commit {
                            history(first: $count) {
                                nodes {
                                    author {
                                        avatarUrl
                                        user {
                                            login
                                        }
                                    }
                                    committer {
                                        name
                                    }

									committer {
										name
									}
									url
                                    message
                                    committedDate
                                    repository {
                                        name
                                        owner {
                                            login
                                        }
                                    }                                    
                                }
                            }
                        }
                    }
                }
            }
        }
        """
        data = self.actor.github_graphql_make_query(
            query, {"owner": owner, "name": repo, "count": 10}
        )

        if not self.is_valid(data):
            logger.warning("[!] Invalid or empty data returned")
            return

        flattened_data = []

        # Extract the commits from the response
        commits = (
            data["data"]["repository"]["defaultBranchRef"]["target"]["history"]["nodes"]
            if data["data"]["repository"]["defaultBranchRef"] is not None
            else []
        )

        # Iterate through the commits and flatten the data
        for commit in commits:
            committer_user = commit.get("author", {}).get("user", None)
            committer_name = None
            if committer_user is None:
                committer_name = commit.get("committer", {}).get("name", None)
            else:
                committer_name = committer_user.get("login", None)
            flattened_commit = {
                "author_avatar_url": commit.get("author", {}).get("avatarUrl", None),
                "author_login": committer_name,
                "message": commit["message"],
                "committed_date": commit["committedDate"],
                "url": commit["url"],
                "repo": commit["repository"]["name"],
                "owner": commit["repository"]["owner"]["login"],
            }
            flattened_data.append(flattened_commit)

        self.get_db_ref(owner, repo).document("recent_commits").set(
            {"data": flattened_data}
        )

    def recent_releases(self, owner, repo, **kwargs):
        # formatting will be in frontend

        flattened_data = []
        cursor = None
        query = """
            query ($owner: String!, $name: String!, $orderBy: ReleaseOrderField!, $cursor: String) {
                repository(owner: $owner, name: $name) {
                    releases(last: 100, before: $cursor, orderBy: {field: $orderBy, direction: DESC}) {
                        pageInfo {
                            endCursor
                            hasNextPage
                        }
                        nodes {
                            name
                            tagName
                            publishedAt
                            url
                            repository {
                                name
                                owner {
                                    login
                                }
                            }                                    
                        }
                    }
                }
            }
            """

        has_next_page = True
        while has_next_page:
            data = self.actor.github_graphql_make_query(
                query,
                {
                    "owner": owner,
                    "name": repo,
                    "cursor": cursor,
                    "orderBy": "CREATED_AT",
                },
            )

            if not self.is_valid(data):
                logger.info(
                    f"[#invalid] No recent releases for repository {owner}/{repo}"
                )
                break

            # Extract the releases from the response
            releases = data["data"]["repository"]["releases"]["nodes"]

            # Iterate through the releases and flatten the data
            for release in releases:
                flattened_release = {
                    "name": release["name"],
                    "tag_name": release["tagName"],
                    "published_at": release["publishedAt"],
                    "url": release["url"],
                    "repo": release["repository"]["name"],
                    "owner": release["repository"]["owner"]["login"],
                }
                flattened_data.append(flattened_release)

            # Check if there is another page of data to fetch
            has_next_page = data["data"]["repository"]["releases"]["pageInfo"][
                "hasNextPage"
            ]
            if not has_next_page:
                break

            # Update the cursor
            cursor = data["data"]["repository"]["releases"]["pageInfo"]["endCursor"]

        self.get_db_ref(owner, repo).document("recent_releases").set(
            {"data": flattened_data}
        )
