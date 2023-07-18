from github_actor import GithubActor
import logging
import log_config
import json
from datetime import datetime
from enum import Enum
import pandas as pd
from datetime import timedelta
import math
import numpy as np
from google.cloud import exceptions
from datetime import timezone
from dateutil import parser

logger = logging.getLogger(__name__)


class GithubWidgets():

    def __init__(self, actor: GithubActor, collection_refs):
        self.actor = actor
        self.collection_refs = collection_refs
        pass

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

        result = self.actor.github_graphql_make_query(
            query, {'owner': owner, 'name': repo})
        if not self.is_valid(result):
            logger.info(f'[#invalid] No repository found for {owner}/{repo}')
            return

        # Flatten the response to a dictionary
        flattened_data = {}
        repository = result['data']['repository']
        flattened_data['default_branch_commit_count'] = repository['defaultBranchRef']['target'][
            'history']['totalCount'] if repository['defaultBranchRef'] is not None else None
        flattened_data['fork_count'] = repository['forkCount']
        flattened_data['stargazer_count'] = repository['stargazerCount']
        flattened_data['created_at'] = repository['createdAt']
        flattened_data['updated_at'] = repository['updatedAt']
        flattened_data['pull_request_count'] = repository['pullRequests']['totalCount']
        flattened_data['commit_comment_count'] = repository['commitComments']['totalCount']
        flattened_data['issue_count'] = repository['issues']['totalCount']
        flattened_data['description'] = repository['description']
        flattened_data['topics'] = [node['topic']['name']
                                    for node in repository['repositoryTopics']['nodes']]
        flattened_data['watcher_count'] = repository['watchers']['totalCount']
        flattened_data['is_fork'] = repository['isFork']
        flattened_data['is_archived'] = repository['isArchived']
        flattened_data['is_empty'] = repository['isEmpty']
        flattened_data['url'] = repository['url']
        flattened_data['owner_login'] = repository['owner']['login']
        flattened_data['owner_avatar_url'] = repository['owner']['avatarUrl']
        flattened_data['release_count'] = repository['releases']['totalCount']
        flattened_data['primary_language_name'] = repository['primaryLanguage'][
            'name'] if repository['primaryLanguage'] is not None else None
        flattened_data['primary_language_color'] = repository['primaryLanguage'][
            'color'] if repository['primaryLanguage'] is not None else None
        flattened_data['environment_count'] = repository['environments']['totalCount']
        flattened_data['disk_usage'] = repository['diskUsage']

        self.collection_refs['widgets'].document(self.get_repo_hash(owner, repo)).update({
            'repository_info': flattened_data
        })

    def _score_repository_activity(self, owner, repo):

        # # We could use an exponential decay function where commits from the current week have a weight of 1, commits from the previous week have a weight of e^(-λ), commits from two weeks ago have a weight of e^(-2λ), and so forth. Here, λ is a decay parameter that you can adjust to change how quickly the weight decreases over time.

        # # Decay parameter
        # lambda_ = 0.1

        # # Time-Weighted Commit Activity Score
        # CAS = sum(math.exp(-lambda_ * i) * commits[i] for i in range(len(commits)))

        # # Normalize CAS (optional)
        # # CAS /= normalization_factor_commits

        # # Time-Weighted Issue Activity Score (IAS): We could weight each issue by a factor that decays with time. For example, we could use an exponential decay function where issues from the current week have a weight of 1, issues from the previous week have a weight of e^(-λ), issues from two weeks ago have a weight of e^(-2λ), and so forth. Here, λ is a decay parameter that you can adjust to change how quickly the weight decreases over time.

        # # List of open and closed issues, each represented as a tuple of (status, date)
        # # where 'status' is either 'open' or 'closed' and 'date' is the date when the issue was opened or resolved.
        # issues = [
        #     ('closed', datetime(2023, 6, 1)),
        #     ('open', datetime(2023, 5, 30)),
        #     ('closed', datetime(2023, 5, 29)),
        #     ('open', datetime(2023, 5, 28)),
        #     # ...
        # ]  # for example

        # # Weights for open and closed issues
        # Wc = 2  # weight for closed issues
        # Wo = 1  # weight for open issues

        # # Decay parameter
        # lambda_ = 0.1

        # # Current date
        # now = datetime.now()

        # # Time-Weighted Issue Activity Score
        # IAS = 0
        # for status, date in issues:
        #     days_ago = (now - date).days
        #     if status == 'closed':
        #         score = Wc / math.sqrt(days_ago + 1)
        #     else:  # status == 'open'
        #         score = Wo / math.sqrt(days_ago + 1)
        #     IAS += math.exp(-lambda_ * days_ago) * score

        # # Time-Weighted Pull Request Activity Score (PRAS): We could weight each pull request by a factor that decays with time. For example, we could use an exponential decay function where pull requests from the current week have a weight of 1, pull requests from the previous week have a weight of e^(-λ), pull requests from two weeks ago have a weight of e^(-2λ), and so forth. Here, λ is a decay parameter that you can adjust to change how quickly the weight decreases over time.

        # # List of open and closed pull requests, each represented as a tuple of (status, date)
        # # where 'status' is either 'open' or 'closed' and 'date' is the date when the pull request was opened or resolved.
        # prs = [
        #     ('closed', datetime(2023, 6, 1)),
        #     ('open', datetime(2023, 5, 30)),
        #     ('closed', datetime(2023, 5, 29)),
        #     ('open', datetime(2023, 5, 28)),
        #     # ...
        # ]  # for example

        # # Weights for open and closed pull requests
        # Wc_pr = 2  # weight for closed pull requests
        # Wo_pr = 1  # weight for open pull requests

        # # Decay parameter
        # lambda_ = 0.1

        # # Current date
        # now = datetime.now()

        # # Time-Weighted Pull Request Activity Score
        # PRAS = 0
        # for status, date in prs:
        #     days_ago = (now - date).days
        #     if status == 'closed':
        #         score = Wc_pr / math.sqrt(days_ago + 1)
        #     else:  # status == 'open'
        #         score = Wo_pr / math.sqrt(days_ago + 1)
        #     PRAS += math.exp(-lambda_ * days_ago) * score

        # -------------------------------------------------------------------
        try:
            ref = self.collection_refs['widgets'].document(self.get_repo_hash(owner, repo)).get(
                field_paths=['recent_releases']).to_dict()

            if ref is None:
                raise exceptions.NotFound('Collection or document not found')

        except exceptions.NotFound as ex:
            # Handle case where document or collection does not exist
            pass

        else:
            releases = ref.get('recent_releases', None)
            if releases:
                # Convert 'published_at' strings to datetime objects and sort in descending order
                release_dates = sorted([parser.isoparse(
                    release['published_at']) for release in releases], reverse=True)

                # Decay parameter
                lambda_ = 0.1

                # Penalty parameter
                penalty_param = 0.01

                # Compute the list of release intervals (in days)
                release_intervals = [
                    (release_dates[i-1] - release_dates[i]).days for i in range(1, len(release_dates))]

                # Compute the standard deviation of the release intervals
                std_dev = np.std(release_intervals)

                # Compute the inverse of the standard deviation, with a small constant added for stability
                inverse_std_dev = 1 / (std_dev + 0.01)

                # Compute the time since the last release (in days)
                time_since_last_release = (datetime.now(
                    timezone.utc) - release_dates[0]).days

                # Penalty factor
                penalty = 1 / (1 + penalty_param * time_since_last_release)

                # Time-Weighted Release Activity Score with Release Interval Consistency and Penalty for Time Since Last Release
                RAS = 0
                for i in range(len(release_dates)):
                    RAS += penalty * math.exp(-lambda_ * i) * inverse_std_dev

    def health_score(self, owner, repo, **kwargs):
        # Repository Activity is calculated by taking the sum of commit activity, issue count, pull request count, and recent releases. This reflects the amount of development activity within the repository.

        # Collaboration is represented by the participation count, number of contributors, and recent pull requests. This indicator is intended to measure the level of collaboration within the project.

        # Popularity can be inferred from recent stargazing activity. This measure represents the community's interest in the project.

        # Code Evolution can be evaluated using code frequency. This parameter shows how frequently the codebase is updated.

        # Community could be measured with the community profile.

        # Diversity can be determined by the language breakdown. This can provide insight into the range of skills and technologies incorporated into the project.

        self._score_repository_activity(owner, repo)

        pass

    def commit_activity(self, owner, repo, **kwargs):
        # formatting will be in frontend
        data = self.actor.github_rest_make_request(
            f'/repos/{owner}/{repo}/stats/commit_activity')

        if not self.is_valid(data):
            logger.info(
                f'[#invalid] No commit activity for repository {owner}/{repo}')
            return

        self.collection_refs['widgets'].document(self.get_repo_hash(owner, repo)).update({
            'commit_activity': data
        })

    def contributors(self, owner, repo, **kwargs):
        # formatting will be in frontend
        data = self.actor.github_rest_make_request(
            f'/repos/{owner}/{repo}/stats/contributors')

        if not self.is_valid(data):
            logger.info(
                f'[#invalid] No contributors for repository {owner}/{repo}')
            return

        # get latest 10 contributors
        self.collection_refs['widgets'].document(self.get_repo_hash(owner, repo)).update({
            'contributors': data
        })

    def participation(self, owner, repo, **kwargs):
        # formatting will be in here
        data = self.actor.github_rest_make_request(
            f'/repos/{owner}/{repo}/stats/participation')

        if not self.is_valid(data):
            logger.info(
                f'[#invalid] No participation for repository {owner}/{repo}')
            return

        # Generate dates for last 52 weeks
        today = datetime.now()
        dates = [(today - timedelta(weeks=i)).strftime('%Y-%m-%d')
                 for i in range(52)][::-1]

        chart_data = {
            "xAxis": {"type": "category", "data": dates},
            "yAxis": {"type": "value"},
            "series": []
        }

        # Subtract 'owner' from 'all' to get 'others'
        others = [all_count - owner_count for all_count,
                  owner_count in zip(data["all"], data["owner"])]

        owner_sum = sum(data["owner"])
        others_sum = sum(others)

        chart_data["series"] = [
            {"name": "All", "data": data["all"], "type": "line"},
            {"name": "Owners", "data": data["owner"], "type": "line"},
            {"name": "Others", "data": others, "type": "line"}
        ]

        self.collection_refs['widgets'].document(self.get_repo_hash(owner, repo)).update({
            'participation': chart_data
        })

        self.collection_refs['widgets'].document(self.get_repo_hash(owner, repo)).update({
            'participation_count': {
                'owner': owner_sum,
                'others': others_sum
            }
        })

    def code_frequency(self, owner, repo, **kwargs):
        # formatting will be in here
        data = self.actor.github_rest_make_request(
            f'/repos/{owner}/{repo}/stats/code_frequency')

        if not self.is_valid(data):
            logger.info(
                f'[#invalid] No code frequency for repository {owner}/{repo}')
            return

        # Define the chart option
        chart_data = {
            "xAxis": {"data": [str(datetime.fromtimestamp(item[0]).date()) for item in data]},
            "yAxis": {},
            "series": [
                {"data": [], "type": "line", "stack": "x"},
                {"data": [], "type": "line", "stack": "x"}
            ],
        }
        chart_data["series"][0]["data"] = [item[1] for item in data]
        chart_data["series"][1]["data"] = [item[2] for item in data]

        self.collection_refs['widgets'].document(self.get_repo_hash(owner, repo)).update({
            'code_frequency': chart_data
        })

    def community_profile(self, owner, repo, **kwargs):
        # formatting will be in frontend
        data = self.actor.github_rest_make_request(
            f'/repos/{owner}/{repo}/community/profile')

        if not self.is_valid(data):
            logger.info(
                f'[#invalid] No community profile for repository {owner}/{repo}')
            return

        self.collection_refs['widgets'].document(self.get_repo_hash(owner, repo)).update({
            'community_profile': data
        })

    def punch_card(self, owner, repo, **kwargs):
        # formatting will be in frontend
        data = self.actor.github_rest_make_request(
            f'/repos/{owner}/{repo}/stats/punch_card')

        if not self.is_valid(data):
            logger.info(
                f'[#invalid] No punch card for repository {owner}/{repo}')
            return

        for i in range(len(data)):
            data[i] = {'day': data[i][0], 'hour': data[i]
                       [1], 'commits': data[i][2]}

        self.collection_refs['widgets'].document(self.get_repo_hash(owner, repo)).update({
            'punch_card': data
        })

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
            query, {'owner': owner, 'name': repo})

        if not self.is_valid(data):
            logger.info(
                f'[#invalid] No issue count for repository {owner}/{repo}')
            return

        self.collection_refs['widgets'].document(self.get_repo_hash(owner, repo)).update({
            'issue_count': {
                'open': data['data']['repository']['openedIssues']['totalCount'],
                'closed': data['data']['repository']['closedIssues']['totalCount']
            }
        })

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
                query, {'owner': owner, 'name': repo, 'cursor': cursor, 'orderBy': 'CREATED_AT'})

            if not self.is_valid(result):
                logger.info(
                    f'[#invalid] No issue activity for repository {owner}/{repo}')
                return

            nodes = result["data"]["repository"]["issues"]["nodes"]
            end_cursor = result["data"]["repository"]["issues"]["pageInfo"]["endCursor"]
            has_next_page = result["data"]["repository"]["issues"]["pageInfo"]["hasNextPage"]

            # Extract relevant data points
            for node in nodes:
                created_at = node["createdAt"]
                closed_at = node["closedAt"]
                closed = node["closed"]
                issues.append({"createdAt": created_at,
                              "closedAt": closed_at, "closed": closed, "commend_count": node["comments"]["totalCount"]})

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
                issue['createdAt'], "%Y-%m-%dT%H:%M:%SZ")

            if closed:
                closed_at_time = datetime.strptime(
                    issue['closedAt'], "%Y-%m-%dT%H:%M:%SZ")
                days_to_close = (closed_at_time - created_at_time).days
                total_days += days_to_close
                closed_issue_count += 1

        self.collection_refs['widgets'].document(self.get_repo_hash(owner, repo)).update({
            'issue_activity': issues
        })

        self.collection_refs['widgets'].document(self.get_repo_hash(owner, repo)).update({
            'average_days_to_close_issue': round(total_days / closed_issue_count, 2)
        })

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
                query, {'owner': owner, 'name': repo, 'cursor': cursor, 'orderBy': 'CREATED_AT'})

            if not self.is_valid(result):
                logger.info(
                    f'[#invalid] No pull request activity for repository {owner}/{repo}')
                return

            nodes = result["data"]["repository"]["pullRequests"]["nodes"]
            end_cursor = result["data"]["repository"]["pullRequests"]["pageInfo"]["endCursor"]
            has_next_page = result["data"]["repository"]["pullRequests"]["pageInfo"]["hasNextPage"]

            # Extract relevant data points
            for node in nodes:
                created_at = node["createdAt"]
                closed_at = node["closedAt"]
                closed = node["closed"]
                pull_requests.append({"createdAt": created_at,
                                      "closedAt": closed_at, "closed": closed, "commend_count": node["comments"]["totalCount"]})

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
                pull_request['createdAt'], "%Y-%m-%dT%H:%M:%SZ")

            if closed:
                closed_at_time = datetime.strptime(
                    pull_request['closedAt'], "%Y-%m-%dT%H:%M:%SZ")
                days_to_close = (closed_at_time - created_at_time).days
                total_days += days_to_close
                closed_pull_request_count += 1

        self.collection_refs['widgets'].document(self.get_repo_hash(owner, repo)).update({
            'pull_request_activity': pull_requests
        })

        self.collection_refs['widgets'].document(self.get_repo_hash(owner, repo)).update({
            'average_days_to_close_pull_request': round(total_days / closed_pull_request_count, 2)
        })

    def most_active_issues(self, owner, repo, **kwargs):
        # formatting will be in frontend

        count = kwargs.get('count', 10)
        flattened_data = {}

        for interval in ['day', 'week', 'month', 'year']:
            flattened_data[interval] = []
            if interval == 'day':
                current_time = datetime.now()
                since = current_time - timedelta(days=1)

            elif interval == 'week':
                current_time = datetime.now()
                since = current_time - timedelta(days=7)

            elif interval == 'month':
                current_time = datetime.now()
                since = current_time - timedelta(days=30)

            elif interval == 'year':
                current_time = datetime.now()
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
                query, {'owner': owner, 'name': repo, 'since': since.isoformat(), 'count': count})

            if not self.is_valid(data):
                logger.info(
                    f'[#invalid] No most active issues for repository {owner}/{repo}')
                continue

            # Extract the issues from the response
            issues = data['data']['repository']['issues']['nodes']

            # Iterate through the issues and flatten the data
            for issue in issues:
                flattened_issue = {
                    'number': issue['number'],
                    'author_avatar_url': issue['author']['avatarUrl'],
                    'author_login': issue['author']['login'],
                    'title': issue['title'],
                    'state': issue['state'],
                    'comments_count': issue['comments']['totalCount'],
                    'closed': issue['closed'],
                    'closed_at': issue['closedAt'],
                    'created_at': issue['createdAt'],
                    'updated_at': issue['updatedAt'],
                    'url': issue['url'],
                    'repo': issue['repository']['name'],
                    'owner': issue['repository']['owner']['login'],
                }
                flattened_data[interval].append(flattened_issue)

        self.collection_refs['widgets'].document(self.get_repo_hash(owner, repo)).update({
            'most_active_issues': flattened_data
        })

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
            query, {'owner': owner, 'name': repo})

        if not self.is_valid(data):
            logger.info(
                f'[#invalid] No pull request count for repository {owner}/{repo}')
            return

        self.collection_refs['widgets'].document(self.get_repo_hash(owner, repo)).update({
            'pull_request_count': {
                'open': data['data']['repository']['openedPullRequests']['totalCount'],
                'closed': data['data']['repository']['closedPullRequests']['totalCount']
            }
        })

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
            query, {'owner': owner, 'name': repo})

        if not self.is_valid(data):
            logger.info(
                f'[#invalid] No language breakdown for repository {owner}/{repo}')
            return

        flattened_data = []

        # Extract the languages from the response
        languages = data['data']['repository']['languages']['edges']

        # Calculate the total size of the codebase
        total_size = sum(language['size'] for language in languages)

        # Iterate through the languages and calculate the percentage of code written in each language
        for language in languages:
            percentage = round(language['size'] / total_size * 100, 2)
            flattened_language = {
                'name': language['node']['name'],
                'percentage': percentage,
                'size': language['size']

            }
            flattened_data.append(flattened_language)

        self.collection_refs['widgets'].document(self.get_repo_hash(owner, repo)).update({
            'language_breakdown': flattened_data
        })

    class RecentIssuesOrder(Enum):
        CREATED_AT = 'CREATED_AT'
        UPDATED_AT = 'UPDATED_AT'

    class RecentPullRequestsOrder(Enum):
        CREATED_AT = 'CREATED_AT'
        UPDATED_AT = 'UPDATED_AT'

    def recent_issues(self, owner, repo, **kwargs):
        # formatting will be in frontend

        order_by = kwargs.get('order_by', self.RecentIssuesOrder.CREATED_AT)

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
            query, {'owner': owner, 'name': repo, 'order_by': order_by.value})

        if not self.is_valid(data):
            logger.info(
                f'[#invalid] No recent issues for repository {owner}/{repo}')
            return

        flattened_data = []

        # Extract the issues from the response
        issues = data['data']['repository']['issues']['nodes']

        # Iterate through the issues and flatten the data
        for issue in issues:
            flattened_issue = {
                'number': issue['number'],
                'author_avatar_url': issue['author']['avatarUrl'],
                'author_login': issue['author']['login'],
                'title': issue['title'],
                'state': issue['state'],
                'comments_count': issue['comments']['totalCount'],
                'created_at': issue['createdAt'],
                'updated_at': issue['updatedAt'],
                'url': issue['url'],
                'repo': issue['repository']['name'],
                'owner': issue['repository']['owner']['login'],
            }
            flattened_data.append(flattened_issue)

        if order_by == self.RecentIssuesOrder.CREATED_AT:
            self.collection_refs['widgets'].document(self.get_repo_hash(owner, repo)).update({
                'recent_created_issues': flattened_data
            })
        elif order_by == self.RecentIssuesOrder.UPDATED_AT:
            self.collection_refs['widgets'].document(self.get_repo_hash(owner, repo)).update({
                'recent_updated_issues': flattened_data
            })

    def recent_pull_requests(self, owner, repo, **kwargs):
        # formatting will be in frontend

        order_by = kwargs.get(
            'order_by', self.RecentPullRequestsOrder.CREATED_AT)
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
            query, {'owner': owner, 'name': repo, 'order_by': order_by.value})

        if not self.is_valid(data):
            logger.info(
                f'[#invalid] No recent pull requests for repository {owner}/{repo}')
            return

        flattened_data = []

        # Extract the pull requests from the response
        pull_requests = data['data']['repository']['pullRequests']['nodes']

        # Iterate through the pull requests and flatten the data
        for pull_request in pull_requests:
            flattened_pull_request = {
                'number': pull_request['number'],
                'author_avatar_url': pull_request['author']['avatarUrl'],
                'author_login': pull_request['author']['login'],
                'title': pull_request['title'],
                'state': pull_request['state'],
                'comments_count': pull_request['comments']['totalCount'],
                'created_at': pull_request['createdAt'],
                'updated_at': pull_request['updatedAt'],
                'url': pull_request['url'],
                'repo': pull_request['repository']['name'],
                'owner': pull_request['repository']['owner']['login'],
            }
            flattened_data.append(flattened_pull_request)

        if order_by == self.RecentPullRequestsOrder.CREATED_AT:
            self.collection_refs['widgets'].document(self.get_repo_hash(owner, repo)).update({
                'recent_created_pull_requests': flattened_data
            })

        elif order_by == self.RecentPullRequestsOrder.UPDATED_AT:
            self.collection_refs['widgets'].document(self.get_repo_hash(owner, repo)).update({
                'recent_updated_pull_requests': flattened_data
            })

    def recent_stargazing_activity(self, owner, repo, **kwargs):
        # formatting will be in here

        max_fetch_pages = kwargs.get('max_fetch_pages', 15)
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
                query, {'owner': owner, 'name': repo, 'cursor': cursor})

            if not self.is_valid(result):
                break

            # Extract the stargazers from the response
            stargazers = result['data']['repository']['stargazers']['edges']

            # Iterate through the stargazers and flatten the data
            for stargazer in stargazers:
                starred_at = stargazer['starredAt']
                cursor = stargazer['cursor']
                if starred_at in star_dict:
                    star_dict[starred_at] += 1
                else:
                    star_dict[starred_at] = 1

            # Check if there is another page of data to fetch
            has_next_page = result['data']['repository']['stargazers']['pageInfo']['hasNextPage']
            current_page += 1

        if len(star_dict) < 0:
            logger.info(
                f'[#invalid] No recent stargazing activity for repository {owner}/{repo}')
            return

        # Convert the dictionary to a Pandas DataFrame
        df = pd.DataFrame.from_dict(
            star_dict, orient='index', columns=['count'])
        df.index = pd.to_datetime(df.index)

        # Group the data by weeks or days depending on the range of dates
        date_range = df.index.max() - df.index.min()
        if date_range > timedelta(days=6 * 30):
            df = df.resample('M').sum()

        elif date_range > timedelta(days=30):
            df = df.resample('W').sum()

        else:
            df = df.resample('D').sum()

        # Convert the datetime index to a string
        df.index = df.index.strftime('%Y-%m-%d')

        # Convert the DataFrame back to a dictionary
        flattened_data = df.to_dict()['count']

        chart_data = {
            "xAxis": {"data": list(flattened_data.keys())},
            "yAxis": {},
            "series": [
                {"data": list(flattened_data.values()),
                 "type": "line", "stack": "x"},
            ],
        }

        self.collection_refs['widgets'].document(self.get_repo_hash(owner, repo)).update({
            'recent_stargazing_activity': chart_data
        })

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
            query, {'owner': owner, 'name': repo, 'count': 10})

        if not self.is_valid(data):
            logger.info(
                f'[#invalid] No recent commits for repository {owner}/{repo}')
            return

        flattened_data = []

        # Extract the commits from the response
        commits = data['data']['repository']['defaultBranchRef']['target']['history'][
            'nodes'] if data['data']['repository']['defaultBranchRef'] is not None else []

        # Iterate through the commits and flatten the data
        for commit in commits:
            committer_user = commit['author'].get('user', None)
            committer_name = None
            if committer_user is None:
                committer_name = commit['committer']['name']
            else:
                committer_name = committer_user['login']
            flattened_commit = {
                'author_avatar_url': commit['author']['avatarUrl'],
                'author_login': committer_name,
                'message': commit['message'],
                'committed_date': commit['committedDate'],
                'url': commit['url'],
                'repo': commit['repository']['name'],
                'owner': commit['repository']['owner']['login'],
            }
            flattened_data.append(flattened_commit)

        self.collection_refs['widgets'].document(self.get_repo_hash(owner, repo)).update({
            'recent_commits': flattened_data
        })

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
                query, {'owner': owner, 'name': repo, 'cursor': cursor, 'orderBy': 'CREATED_AT'})

            if not self.is_valid(data):
                logger.info(
                    f'[#invalid] No recent releases for repository {owner}/{repo}')
                break

            # Extract the releases from the response
            releases = data['data']['repository']['releases']['nodes']

            # Iterate through the releases and flatten the data
            for release in releases:
                flattened_release = {
                    'name': release['name'],
                    'tag_name': release['tagName'],
                    'published_at': release['publishedAt'],
                    'url': release['url'],
                    'repo': release['repository']['name'],
                    'owner': release['repository']['owner']['login'],
                }
                flattened_data.append(flattened_release)

            # Check if there is another page of data to fetch
            has_next_page = data['data']['repository']['releases']['pageInfo']['hasNextPage']
            if not has_next_page:
                break

            # Update the cursor
            cursor = data['data']['repository']['releases']['pageInfo']['endCursor']

        self.collection_refs['widgets'].document(self.get_repo_hash(owner, repo)).update({
            'recent_releases': flattened_data
        })
