from github_actor import GithubActor
import logging
import log_config
import json
from datetime import datetime
from enum import Enum
import pandas as pd
from datetime import timedelta
from itertools import zip_longest
from collections import defaultdict
from sklearn.preprocessing import MinMaxScaler
import numpy as np
from sklearn.preprocessing import StandardScaler
from scipy.stats import norm
from sklearn.preprocessing import RobustScaler, quantile_transform


logger = logging.getLogger(__name__)


class GithubCumulative():

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

    def cumulative_stats(self, **kwargs):
        docs = self.collection_refs['widgets'].stream()
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

        for doc in docs:
            repo_info = doc.to_dict().get('repository_info')
            if repo_info is None:
                continue
            cum_disk_usage += repo_info.get('disk_usage', 0)
            cum_commit_comment_count += repo_info.get(
                'commit_comment_count', 0)

            branch_commit_count = repo_info.get(
                'default_branch_commit_count', 0)

            cum_default_branch_commit_count += branch_commit_count if branch_commit_count is not None else 0

            cum_environment_count += repo_info.get('environment_count', 0)
            cum_fork_count += repo_info.get('fork_count', 0)
            cum_pull_request_count += repo_info.get('pull_request_count', 0)
            cum_issue_count += repo_info.get('issue_count', 0)
            cum_release_count += repo_info.get('release_count', 0)
            cum_stargazers_count += repo_info.get('stargazers_count', 0)
            cum_watcher_count += repo_info.get('watcher_count', 0)

        self.collection_refs['cumulative'].document('cumulative_info').set({'data': {
            'disk_usage': cum_disk_usage,
            'commit_comment_count': cum_commit_comment_count,
            'default_branch_commit_count': cum_default_branch_commit_count,
            'environment_count': cum_environment_count,
            'fork_count': cum_fork_count,
            'pull_request_count': cum_pull_request_count,
            'issue_count': cum_issue_count,
            'release_count': cum_release_count,
            'stargazers_count': cum_stargazers_count,
            'watcher_count': cum_watcher_count
        }})

    def cumulative_commit_activity(self, **kwargs):

        docs = self.collection_refs['widgets'].stream()

        d1 = None
        for doc in docs:
            d2 = doc.to_dict().get('commit_activity')
            if d2 is None:
                continue
            if d1 is None:
                d1 = d2
            else:
                d1 = [{
                    "days": [x + y for x, y in zip(d1["days"], d2["days"])],
                    "total": d1["total"] + d2["total"],
                    "week": d1["week"]
                } for d1, d2 in zip(d1, d2)]

        self.collection_refs['cumulative'].document(
            'cumulative_commit_activity').set({'data': d1})

    def normalize_health_score(self, **kwargs):

        # Fetching sub-collections
        collection_docs = self.collection_refs['widgets'].document(
            'repositories').collections()

        raw_scores = {}
        for sub_collection in collection_docs:
            doc_name = sub_collection.id
            raw_health_score_doc = sub_collection.document(
                'raw_health_score').get()

            if not raw_health_score_doc.exists:
                continue

            data = raw_health_score_doc.to_dict().get('data', None)
            if data is None:
                continue
            for key, value in data.items():
                if key not in raw_scores:
                    raw_scores[key] = []
                raw_scores[key].append((doc_name, value))

        # New dictionary to store normalized scores
        new_scores = {}

        for key, values in raw_scores.items():
            names, scores = zip(*values)  # Unpack repo names and scores

            total_score = sum(scores)
            proportional_scores = [(name, score / total_score * 100)
                                   for name, score in zip(names, scores)]

            # Add each score to the new_scores dictionary
            for name, score in proportional_scores:
                if name not in new_scores:
                    new_scores[name] = {}
                new_scores[name][key] = score

        for doc_name, scores in new_scores.items():
            for key, value in scores.items():
                if isinstance(value, float):
                    scores[key] = round(value, 2)

            scores['total'] = sum(scores.values()) / len(scores.values())

            self.collection_refs['widgets'].document('repositories').collection(
                doc_name).document('health_score').set({'data': scores})

    def cumulative_participation(self, **kwargs):

        docs = self.collection_refs['widgets'].stream()

        d1 = None
        for doc in docs:
            d2 = doc.to_dict().get('participation')
            if d2 is None:
                continue
            if d1 is None:
                d1 = d2
            else:
                d1 = {
                    "xAxis": {"type": "category"},
                    "yAxis": {"type": "value"},
                    "series": [
                        {"name": s1["name"], "data": [
                            x + y for x, y in zip(s1["data"], s2["data"])], "type": "line"}
                        for s1, s2 in zip(d1["series"], d2["series"])
                    ]
                }

        self.collection_refs['cumulative'].document(
            'cumulative_participation').set({'data': d1})

    def cumulative_code_frequency(self, **kwargs):

        def aggregate_sum(data1, data2):
            # Creating a default dictionary
            result = defaultdict(lambda: defaultdict(int))

            # Summing the series data with same date
            for data in [data1, data2]:
                for i, date in enumerate(data["xAxis"]['data']):
                    for j, series in enumerate(data["series"]):
                        result[date][j] += series["data"][i]

            # Creating the final data
            final_data = {
                "xAxis": {'data': []},
                "yAxis": {},
                "series": [{"data": [], "type": "line", "stack": "x"} for _ in range(len(data1["series"]))],
            }

            for date, series_dict in sorted(result.items()):
                final_data["xAxis"]['data'].append(date)
                for i, val in series_dict.items():
                    final_data["series"][i]["data"].append(val)

            return final_data

        docs = self.collection_refs['widgets'].stream()

        data1 = None
        for doc in docs:
            data2 = doc.to_dict().get('code_frequency')
            if data2 is None:
                continue
            if data1 is None:
                data1 = data2
            else:
                data1 = aggregate_sum(data1, data2)

        self.collection_refs['cumulative'].document(
            'cumulative_code_frequency').set({'data': data1})

    def cumulative_punch_card(self, **kwargs):

        docs = self.collection_refs['widgets'].stream()

        data1 = None
        for doc in docs:
            data2 = doc.to_dict().get('punch_card')
            if data2 is None:
                continue
            if data1 is None:
                data1 = data2
            else:
                for i in range(len(data1)):
                    data1[i]['commits'] = data1[i]['commits'] + \
                        data2[i]['commits']

        self.collection_refs['cumulative'].document(
            'cumulative_punch_card').set({'data': data1})

    def cumulative_issue_count(self, **kwargs):

        docs = self.collection_refs['widgets'].stream()

        cumulative_open_sum = 0
        cumulative_closed_sum = 0

        for doc in docs:
            data = doc.to_dict().get('issue_count')
            if data is None:
                continue

            cumulative_open_sum += data['open']
            cumulative_closed_sum += data['closed']

        self.collection_refs['cumulative'].document('cumulative_issue_count').set({'data': {
            'open': cumulative_open_sum,
            'closed': cumulative_closed_sum
        }})

    def cumulative_most_active_issues(self, **kwargs):
        docs = self.collection_refs['widgets'].stream()
        dayly = []
        weekly = []
        monthly = []
        yearly = []

        for doc in docs:
            data = doc.to_dict().get('most_active_issues')
            if data is None:
                continue

        if data['day']:
            dayly.extend(data['day'])
            dayly = sorted(
                dayly, key=lambda x: x['comments_count'], reverse=True)[:10]

        if data['week']:
            weekly.extend(data['week'])
            weekly = sorted(
                weekly, key=lambda x: x['comments_count'], reverse=True)[:10]

        if data['month']:
            monthly.extend(data['month'])
            monthly = sorted(
                monthly, key=lambda x: x['comments_count'], reverse=True)[:10]

        if data['year']:
            yearly.extend(data['year'])
            yearly = sorted(
                yearly, key=lambda x: x['comments_count'], reverse=True)[:10]

        self.collection_refs['cumulative'].document('cumulative_most_active_issues').set({'data': {
            'day': dayly,
            'week': weekly,
            'month': monthly,
            'year': yearly
        }})

    def cumulative_pull_request_count(self, **kwargs):

        docs = self.collection_refs['widgets'].stream()

        cumulative_open_sum = 0
        cumulative_closed_sum = 0

        for doc in docs:
            data = doc.to_dict().get('pull_request_count')
            if data is None:
                continue

            cumulative_open_sum += data['open']
            cumulative_closed_sum += data['closed']

        self.collection_refs['cumulative'].document('cumulative_pull_request_count').set({'data': {
            'open': cumulative_open_sum,
            'closed': cumulative_closed_sum
        }})

    def cumulative_language_breakdown(self, **kwargs):

        docs = self.collection_refs['widgets'].stream()

        cumulative_data = {}

        for doc in docs:
            data = doc.to_dict().get('language_breakdown')
            if data is None:
                continue

            for language in data:
                if language['name'] in cumulative_data:
                    cumulative_data[language['name']] += language['size']
                else:
                    cumulative_data[language['name']] = language['size']

        total_size = sum(cumulative_data.values())

        cumulative_flatten = []
        for cumulative_language_name, cumulative_language_size in cumulative_data.items():
            percentage = round(cumulative_language_size / total_size * 100, 2)
            flattened_language = {
                'name': cumulative_language_name,
                'percentage': percentage,
                'size': cumulative_language_size

            }
            cumulative_flatten.append(flattened_language)

        self.collection_refs['cumulative'].document(
            'cumulative_language_breakdown').set({'data': cumulative_flatten})

    class CumulativeRecentIssuesOrder(Enum):
        CREATED_AT = 'CREATED_AT'
        UPDATED_AT = 'UPDATED_AT'

    def cumulative_recent_issues(self, **kwargs):
        docs = self.collection_refs['widgets'].stream()

        order_by = kwargs.get(
            'order_by', self.CumulativeRecentIssuesOrder.CREATED_AT)
        
        if order_by == self.CumulativeRecentIssuesOrder.CREATED_AT:
            field_name = 'recent_created_issues'

        elif order_by == self.CumulativeRecentIssuesOrder.UPDATED_AT:
            field_name = 'recent_updated_issues'

        cumulative_recent_issues = []
        for doc in docs:
            data = doc.to_dict().get(field_name)
            if data is None:
                continue

            cumulative_recent_issues.extend(data)
            cumulative_recent_issues = sorted(
                data, key=lambda x: x[order_by], reverse=True)[:10]

        self.collection_refs['cumulative'].document(
            f'cumulative_{field_name}').set({'data': cumulative_recent_issues})

    class CumulativeRecentPullRequestsOrder(Enum):
        CREATED_AT = 'CREATED_AT'
        UPDATED_AT = 'UPDATED_AT'

    def cumulative_recent_pull_requests(self, **kwargs):
        docs = self.collection_refs['widgets'].stream()

        order_by = kwargs.get(
            'order_by', self.CumulativeRecentIssuesOrder.CREATED_AT)

        if order_by == self.CumulativeRecentIssuesOrder.CREATED_AT:
            field_name = 'recent_created_pull_requests'

        elif order_by == self.CumulativeRecentIssuesOrder.UPDATED_AT:
            field_name = 'recent_updated_pull_requests'

        cumulative_recent_issues = []
        for doc in docs:
            data = doc.to_dict().get(field_name)
            if data is None:
                continue

            cumulative_recent_issues.extend(data)
            cumulative_recent_issues = sorted(
                data, key=lambda x: x[order_by], reverse=True)[:10]

        self.collection_refs['cumulative'].document(
            f'cumulative_{field_name}').set({'data': cumulative_recent_issues})

    def cumulative_recent_commits(self, **kwargs):
        docs = self.collection_refs['widgets'].stream()

        cumulative_recent_commits = []
        for doc in docs:
            data = doc.to_dict().get('recent_commits')
            if data is None:
                continue

            cumulative_recent_commits.extend(data)
            cumulative_recent_commits = sorted(
                data, key=lambda x: x['committed_date'], reverse=True)[:10]

        self.collection_refs['cumulative'].document(
            f'cumulative_recent_commits').set({'data': cumulative_recent_commits})

    def cumulative_recent_releases(self, **kwargs):
        docs = self.collection_refs['widgets'].stream()

        cumulative_recent_releases = []
        for doc in docs:
            data = doc.to_dict().get('recent_releases')
            if data is None:
                continue

            cumulative_recent_releases.extend(data)
            cumulative_recent_releases = sorted(
                data, key=lambda x: x['published_at'], reverse=True)[:10]

        self.collection_refs['cumulative'].document(
            f'cumulative_recent_releases').set({'data': cumulative_recent_releases})
