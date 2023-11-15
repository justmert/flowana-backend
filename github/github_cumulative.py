from .github_actor import GithubActor
import logging
from enum import Enum
from collections import defaultdict
import numpy as np
from sklearn.preprocessing import RobustScaler
import datetime
from datetime import timedelta
from scipy.stats import norm


logger = logging.getLogger(__name__)


class GithubCumulative:
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

    def cumulative_stats(self, **kwargs) -> None:
        cum_disk_usage = 0
        cum_commit_comment_count = 0
        cum_default_branch_commit_count = 0
        cum_environment_count = 0
        cum_fork_count = 0
        cum_pull_request_count = 0
        cum_issue_count = 0
        cum_release_count = 0
        cum_stargazers_count = 0
        cum_watcher_count = 0

        repo_doc = self.collection_refs["widgets"].document("repositories")
        for subcollection in repo_doc.collections():
            doc_val = subcollection.document("repository_info").get()

            if not doc_val.exists:
                continue

            data = doc_val.to_dict().get("data", None)
            cum_disk_usage += data.get("disk_usage", 0)
            cum_commit_comment_count += data.get("commit_comment_count", 0)

            branch_commit_count = data.get("default_branch_commit_count", 0)

            cum_default_branch_commit_count += (
                branch_commit_count if branch_commit_count is not None else 0
            )
            cum_environment_count += data.get("environment_count", 0)
            cum_fork_count += data.get("fork_count", 0)
            cum_pull_request_count += data.get("pull_request_count", 0)
            cum_issue_count += data.get("issue_count", 0)
            cum_release_count += data.get("release_count", 0)
            cum_stargazers_count += data.get("stargazers_count", 0)
            cum_watcher_count += data.get("watcher_count", 0)

        self.collection_refs["cumulative"].document("cumulative_info").set(
            {
                "data": {
                    "disk_usage": cum_disk_usage,
                    "commit_comment_count": cum_commit_comment_count,
                    "default_branch_commit_count": cum_default_branch_commit_count,
                    "environment_count": cum_environment_count,
                    "fork_count": cum_fork_count,
                    "pull_request_count": cum_pull_request_count,
                    "issue_count": cum_issue_count,
                    "release_count": cum_release_count,
                    "stargazers_count": cum_stargazers_count,
                    "watcher_count": cum_watcher_count,
                }
            }
        )

    def cumulative_commit_activity(self, **kwargs):
        d1 = None
        repo_doc = self.collection_refs["widgets"].document("repositories")
        for subcollection in repo_doc.collections():
            doc_val = subcollection.document("commit_activity").get()

            if not doc_val.exists:
                continue

            d2 = doc_val.to_dict().get("data", None)

            if d2 is None:
                continue
            if d1 is None:
                d1 = d2
            else:
                d1 = [
                    {
                        "days": [x + y for x, y in zip(d1["days"], d2["days"])],
                        "total": d1["total"] + d2["total"],
                        "week": d1["week"],
                    }
                    for d1, d2 in zip(d1, d2)
                ]

        self.collection_refs["cumulative"].document("cumulative_commit_activity").set(
            {"data": d1}
        )

    def normalize_health_score(self, **kwargs):
        weight_dict = {
            "commit_activity": 0.24,
            "issue_activity": 0.18,
            "pull_request_activity": 0.18,
            "release_activity": 0.12,
            "contribution_activity": 0.28,
        }
        grades_boundaries = [
            "S+",
            "S",
            "A+",
            "A",
            "B+",
            "B",
            "C+",
            "C",
            "D+",
            "D",
            "E+",
            "E",
        ]

        collection_docs = (
            self.collection_refs["widgets"].document("repositories").collections()
        )

        raw_scores = {}
        for sub_collection in collection_docs:
            doc_name = sub_collection.id
            raw_health_score_doc = sub_collection.document("raw_health_score").get()

            if not raw_health_score_doc.exists:
                continue

            data = raw_health_score_doc.to_dict().get("data", None)
            if data is None:
                continue
            for key, value in data.items():
                if key not in raw_scores:
                    raw_scores[key] = []
                raw_scores[key].append((doc_name, value))

        # Initialize the robust scaler
        robust_scaler = RobustScaler()

        # Initialize a new dictionary to hold the normalized scores
        new_scores = {}

        # Iterate over each feature in the raw_scores dictionary
        for key, values in raw_scores.items():
            # Extract names and scores
            names, scores = zip(*values)  # Unpack repo names and scores

            # Convert the list of scores to a numpy array and reshape for the scaler
            scores_array = np.array(scores).reshape(-1, 1)

            # Mask for non-zero entries
            non_zero_mask = scores_array != 0
            # Extract non-zero scores for scaling
            non_zero_scores = scores_array[non_zero_mask]

            # Reshape non_zero_scores to a 2D array for fitting the scaler
            if non_zero_scores.ndim == 1:
                non_zero_scores = non_zero_scores.reshape(-1, 1)

            # Initialize an array to hold the normalized scores
            normalized_scores_array = np.zeros_like(scores_array)

            # Fit and transform non-zero scores
            if non_zero_scores.size > 0:  # Check if there are any non-zero scores
                robust_scaler.fit(non_zero_scores)
                # Normalize the non-zero scores
                normalized_non_zero_scores = robust_scaler.transform(
                    non_zero_scores
                ).flatten()
                # Assign normalized scores back to their corresponding positions in the normalized array
                normalized_scores_array[non_zero_mask] = normalized_non_zero_scores

            # Convert the non-zero normalized values to percentiles
            percentiles = np.zeros_like(scores_array)
            percentiles[non_zero_mask] = (
                norm.cdf(normalized_scores_array[non_zero_mask].flatten()) * 100
            )

            # Flatten the percentiles array to convert it to 1D
            percentiles = percentiles.flatten()

            # Round the percentiles and create a list of (name, score)
            normalized_scores = list(zip(names, [round(p, 2) for p in percentiles]))

            # Add each normalized score to the new_scores dictionary
            for name, score in normalized_scores:
                if name not in new_scores:
                    new_scores[name] = {}
                new_scores[name][key] = score

        for doc_name, scores in new_scores.items():
            print(scores)
            # Calculate the total score based on the weighted sum of the individual scores
            scores["total"] = sum(
                scores[key] * weight_dict.get(key, 0) for key in scores.keys()
            )

        # Assuming `all_total_scores` contains the 'total' score of each repository
        all_total_scores = [scores["total"] for scores in new_scores.values()]

        # Calculate the percentile values for each grade
        percentiles_to_compute = [
            100 - (10 / len(all_total_scores)),
            90,
            80,
            70,
            60,
            50,
            40,
            30,
            20,
            10,
            5,
            2,
        ]

        # Calculate the percentiles for all boundaries in one go
        percentile_values = np.percentile(all_total_scores, percentiles_to_compute)

        # Map the percentile values to their corresponding grades

        # Create a dictionary mapping the grade to its percentile boundary
        percentile_bounds = dict(zip(grades_boundaries, percentile_values))

        # Add the lower bound for "F"
        percentile_bounds["F"] = 0

        # Function to determine the grade based on score
        def determine_grade(score, percentile_bounds):
            for grade, bound in sorted(
                percentile_bounds.items(), key=lambda x: x[1], reverse=True
            ):
                if score >= bound:
                    return grade
            return "F"  # If for some reason the score does not exceed the lowest bound

        # Assign grades to each document based on the total score and calculated percentiles
        for doc_name, scores in new_scores.items():
            # Calculate the total score based on the weighted sum of the individual scores
            scores["total"] = sum(
                scores[key] * weight_dict.get(key, 0) for key in scores.keys()
            )

            # Determine the grade based on the total score
            scores["grade"] = determine_grade(scores["total"], percentile_bounds)

            # Update the documents in the database with the new scores and grades
            self.collection_refs["widgets"].document("repositories").collection(
                doc_name
            ).document("health_score").set({"data": scores})
            self.collection_refs["projects"].document(doc_name).set(
                {"health_score": scores}, merge=True
            )

    def cumulative_participation(self, **kwargs):
        d1 = None
        repo_doc = self.collection_refs["widgets"].document("repositories")
        for subcollection in repo_doc.collections():
            doc_val = subcollection.document("participation").get()

            if not doc_val.exists:
                continue

            d2 = doc_val.to_dict().get("data", None)
            today_utc = (
                datetime.datetime.utcnow()
            )  # Switch from datetime.now() to datetime.utcnow()

            if d2 is None:
                continue
            if d1 is None:
                d1 = d2
            else:
                d1 = {
                    "xAxis": {
                        "type": "category",
                        "data": [
                            (today_utc - timedelta(weeks=i)).strftime("%Y-%m-%d")
                            for i in range(52)
                        ][::-1],
                    },
                    "yAxis": {"type": "value"},
                    "series": [
                        {
                            "name": s1["name"],
                            "data": [x + y for x, y in zip(s1["data"], s2["data"])],
                            "type": "line",
                        }
                        for s1, s2 in zip(d1["series"], d2["series"])
                    ],
                }

        self.collection_refs["cumulative"].document("cumulative_participation").set(
            {"data": d1}
        )

    def cumulative_code_frequency(self, **kwargs):
        def aggregate_sum(data1, data2):
            # Creating a default dictionary
            result = defaultdict(lambda: defaultdict(int))

            # Summing the series data with same date
            for data in [data1, data2]:
                for i, date in enumerate(data["xAxis"]["data"]):
                    for j, series in enumerate(data["series"]):
                        result[date][j] += series["data"][i]

            # Creating the final data
            final_data = {
                "xAxis": {"data": []},
                "yAxis": {},
                "series": [
                    {"data": [], "type": "line", "stack": "x"}
                    for _ in range(len(data1["series"]))
                ],
            }

            for date, series_dict in sorted(result.items()):
                final_data["xAxis"]["data"].append(date)
                for i, val in series_dict.items():
                    final_data["series"][i]["data"].append(val)

            return final_data

        # docs = self.collection_refs['widgets'].stream()

        d1 = None
        repo_doc = self.collection_refs["widgets"].document("repositories")
        for subcollection in repo_doc.collections():
            doc_val = subcollection.document("code_frequency").get()

            if not doc_val.exists:
                continue

            d2 = doc_val.to_dict().get("data", None)
            if d2 is None:
                continue
            if d1 is None:
                d1 = d2
            else:
                d1 = aggregate_sum(d1, d2)

        self.collection_refs["cumulative"].document("cumulative_code_frequency").set(
            {"data": d1}
        )

    def cumulative_punch_card(self, **kwargs):
        d1 = None
        repo_doc = self.collection_refs["widgets"].document("repositories")
        for subcollection in repo_doc.collections():
            doc_val = subcollection.document("punch_card").get()

            if not doc_val.exists:
                continue

            d2 = doc_val.to_dict().get("data", None)
            if d2 is None:
                continue
            if d1 is None:
                d1 = d2
            else:
                for i in range(len(d1)):
                    d1[i]["commits"] = d1[i]["commits"] + d2[i]["commits"]

        self.collection_refs["cumulative"].document("cumulative_punch_card").set(
            {"data": d1}
        )

    def cumulative_issue_count(self, **kwargs):
        cumulative_open_sum = 0
        cumulative_closed_sum = 0

        repo_doc = self.collection_refs["widgets"].document("repositories")
        for subcollection in repo_doc.collections():
            doc_val = subcollection.document("issue_count").get()

            if not doc_val.exists:
                continue

            data = doc_val.to_dict().get("data", None)

            cumulative_open_sum += data["open"]
            cumulative_closed_sum += data["closed"]

        self.collection_refs["cumulative"].document("cumulative_issue_count").set(
            {
                "data": {
                    "open": cumulative_open_sum,
                    "closed": cumulative_closed_sum,
                }
            }
        )

    def cumulative_most_active_issues(self, **kwargs):
        dayly = []
        weekly = []
        monthly = []
        yearly = []

        repo_doc = self.collection_refs["widgets"].document("repositories")
        for subcollection in repo_doc.collections():
            doc_val = subcollection.document("most_active_issues").get()

            if not doc_val.exists:
                continue

            data = doc_val.to_dict().get("data", None)

            if data.get("day", None):
                dayly.extend(data["day"])
                dayly = sorted(dayly, key=lambda x: x["comments_count"], reverse=True)[
                    :10
                ]

            if data.get("week", None):
                weekly.extend(data["week"])
                weekly = sorted(
                    weekly, key=lambda x: x["comments_count"], reverse=True
                )[:10]

            if data.get("month", None):
                monthly.extend(data["month"])
                monthly = sorted(
                    monthly, key=lambda x: x["comments_count"], reverse=True
                )[:10]

            if data.get("year", None):
                yearly.extend(data["year"])
                yearly = sorted(
                    yearly, key=lambda x: x["comments_count"], reverse=True
                )[:10]

        self.collection_refs["cumulative"].document(
            "cumulative_most_active_issues"
        ).set(
            {
                "data": {
                    "day": dayly,
                    "week": weekly,
                    "month": monthly,
                    "year": yearly,
                }
            }
        )

    def cumulative_pull_request_count(self, **kwargs):
        cumulative_open_sum = 0
        cumulative_closed_sum = 0

        repo_doc = self.collection_refs["widgets"].document("repositories")
        for subcollection in repo_doc.collections():
            doc_val = subcollection.document("pull_request_count").get()

            if not doc_val.exists:
                continue

            data = doc_val.to_dict().get("data", None)

            cumulative_open_sum += data["open"]
            cumulative_closed_sum += data["closed"]

        self.collection_refs["cumulative"].document(
            "cumulative_pull_request_count"
        ).set(
            {
                "data": {
                    "open": cumulative_open_sum,
                    "closed": cumulative_closed_sum,
                }
            }
        )

    def cumulative_language_breakdown(self, **kwargs):
        cumulative_data = {}

        repo_doc = self.collection_refs["widgets"].document("repositories")
        for subcollection in repo_doc.collections():
            doc_val = subcollection.document("language_breakdown").get()

            if not doc_val.exists:
                continue

            data = doc_val.to_dict().get("data", None)

            for language in data:
                if language["name"] in cumulative_data:
                    cumulative_data[language["name"]] += language["size"]
                else:
                    cumulative_data[language["name"]] = language["size"]

        total_size = sum(cumulative_data.values())

        cumulative_flatten = []
        for (
            cumulative_language_name,
            cumulative_language_size,
        ) in cumulative_data.items():
            percentage = round(cumulative_language_size / total_size * 100, 2)
            flattened_language = {
                "name": cumulative_language_name,
                "percentage": percentage,
                "size": cumulative_language_size,
            }
            cumulative_flatten.append(flattened_language)

        self.collection_refs["cumulative"].document(
            "cumulative_language_breakdown"
        ).set({"data": cumulative_flatten})

    class CumulativeRecentIssuesOrder(Enum):
        CREATED_AT = "CREATED_AT"
        UPDATED_AT = "UPDATED_AT"

    def cumulative_recent_issues(self, **kwargs):
        order_by = kwargs.get("order_by", self.CumulativeRecentIssuesOrder.CREATED_AT)

        if order_by == self.CumulativeRecentIssuesOrder.CREATED_AT:
            field_name = "recent_created_issues"

        elif order_by == self.CumulativeRecentIssuesOrder.UPDATED_AT:
            field_name = "recent_updated_issues"

        cumulative_recent_issues = []
        repo_doc = self.collection_refs["widgets"].document("repositories")
        for subcollection in repo_doc.collections():
            doc_val = subcollection.document(field_name).get()

            if not doc_val.exists:
                continue

            data = doc_val.to_dict().get("data", None)
            cumulative_recent_issues.extend(data)
            cumulative_recent_issues = sorted(
                cumulative_recent_issues,
                key=lambda x: x[order_by.value.lower()],
                reverse=True,
            )[:10]

        self.collection_refs["cumulative"].document(f"cumulative_{field_name}").set(
            {"data": cumulative_recent_issues}
        )

    class CumulativeRecentPullRequestsOrder(Enum):
        CREATED_AT = "CREATED_AT"
        UPDATED_AT = "UPDATED_AT"

    def cumulative_recent_pull_requests(self, **kwargs):
        order_by = kwargs.get(
            "order_by", self.CumulativeRecentPullRequestsOrder.CREATED_AT
        )

        if order_by == self.CumulativeRecentPullRequestsOrder.CREATED_AT:
            field_name = "recent_created_pull_requests"

        elif order_by == self.CumulativeRecentPullRequestsOrder.UPDATED_AT:
            field_name = "recent_updated_pull_requests"

        cumulative_recent_issues = []
        repo_doc = self.collection_refs["widgets"].document("repositories")
        for subcollection in repo_doc.collections():
            doc_val = subcollection.document(field_name).get()

            if not doc_val.exists:
                continue

            data = doc_val.to_dict().get("data", None)

            cumulative_recent_issues.extend(data)
            cumulative_recent_issues = sorted(
                cumulative_recent_issues,
                key=lambda x: x[order_by.value.lower()],
                reverse=True,
            )[:10]

        self.collection_refs["cumulative"].document(f"cumulative_{field_name}").set(
            {"data": cumulative_recent_issues}
        )

    def cumulative_recent_commits(self, **kwargs):
        cumulative_recent_commits = []
        repo_doc = self.collection_refs["widgets"].document("repositories")
        for subcollection in repo_doc.collections():
            doc_val = subcollection.document("recent_commits").get()

            if not doc_val.exists:
                continue

            data = doc_val.to_dict().get("data", None)

            cumulative_recent_commits.extend(data)
            cumulative_recent_commits = sorted(
                cumulative_recent_commits,
                key=lambda x: x["committed_date"],
                reverse=True,
            )[:10]

        self.collection_refs["cumulative"].document("cumulative_recent_commits").set(
            {"data": cumulative_recent_commits}
        )

    def cumulative_recent_releases(self, **kwargs):
        cumulative_recent_releases = []
        repo_doc = self.collection_refs["widgets"].document("repositories")
        for subcollection in repo_doc.collections():
            doc_val = subcollection.document("recent_releases").get()

            if not doc_val.exists:
                continue

            data = doc_val.to_dict().get("data", None)
            cumulative_recent_releases.extend(data)
            cumulative_recent_releases = sorted(
                cumulative_recent_releases,
                key=lambda x: x["published_at"],
                reverse=True,
            )[:10]

        self.collection_refs["cumulative"].document("cumulative_recent_releases").set(
            {"data": cumulative_recent_releases}
        )
