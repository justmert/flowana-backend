from request_actor import Actor
import logging
import log_config
import json
from datetime import datetime
from enum import Enum
import pandas as pd
from datetime import timedelta

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, app, db, actor: Actor):
        self.app = app
        self.db = db
        self.actor = actor
        self.repositories = []
        self.pipeline_functions = []
        self.collection_refs = {}

    def contruct_pipeline(self, protocol_name, collection_refs):
        self.collection_refs = collection_refs
        self.protocol_name = protocol_name
        repositories_ref_stream = self.db.collection(
            f'{protocol_name}-projects').stream()

        self.repositories = []

        # Print the data for each document
        for repository in repositories_ref_stream:
            self.repositories.append(repository.to_dict())

        logger.info(
            f'Found {len(self.repositories)} repositories for protocol {protocol_name} to be updated.')

        self.pipeline_functions = [  # pipeline function, params
            (self.repository_info, ()),
            (self.commit_activity, ()),
            (self.participation, ()),
            (self.code_frequency, ()),
            (self.commit_participation, ()),
            (self.punch_card, ()),
            (self.community_profile, ()),
            (self.language_breakdown, ()),
            
            (self.issue_count, ()),
            (self.issue_activity, ()),
            (self.most_active_issues, ()),
            
            (self.pull_request_count, ()),
            (self.pull_request_activity, ()),
            # (self.most_active_pull_requests, ()), #FIXME: needs manual process

            (self.recent_issues, ()),
            (self.recent_pull_requests, ()),
            (self.recent_stargazing_activity, ()),
            (self.recent_commits, ()),
            (self.recent_releases, ()),
        ]

    def run_pipeline(self):
        # for repository in self.repositories:
        repository = {'owner': 'lensterxyz', 'repo': 'lenster'}

        is_valid = self.actor.check_repo_validity(
            repository['owner'], repository['repo'])
        if not is_valid:
            logger.info(
                f'Repository {repository["owner"]}/{repository["repo"]} is not valid.')
            # continue
        logger.info(
            f'Running pipeline for repository {repository["owner"]}/{repository["repo"]}')

        for func, args in self.pipeline_functions:
            logging.info(f'[*] Running pipeline function: {func.__name__}')
            func(repository['owner'], repository['repo'], *args)

        logger.info(
            f'Finished running pipeline for repository {repository["owner"]}/{repository["repo"]}')

    def is_response_valid(self, response):
        if response is None:
            return False

        elif isinstance(response, list) and len(response) == 0:
            return False

        return True

    def get_repo_hash(self, owner, repo):
        return "#".join([owner, repo])

    def repository_info(self, owner: str, repo: str):
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
        is_valid = self.is_response_valid(result)
        if not is_valid:
            logger.info(f'No repository found for {owner}/{repo}')
            return

        # Flatten the response to a dictionary
        flattened_data = {}
        repository = result['data']['repository']
        flattened_data['default_branch_commit_count'] = repository['defaultBranchRef']['target']['history']['totalCount']
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
        flattened_data['primary_language_name'] = repository['primaryLanguage']['name']
        flattened_data['primary_language_color'] = repository['primaryLanguage']['color']
        flattened_data['environment_count'] = repository['environments']['totalCount']
        flattened_data['disk_usage'] = repository['diskUsage']

        self.collection_refs['widgets'].document(self.get_repo_hash(owner, repo)).set({
            'repository_info': flattened_data
        }, merge=True)

    def commit_activity(self, owner, repo):
        # formatting will be in frontend
        data = self.actor.github_rest_make_request(
            f'/repos/{owner}/{repo}/stats/commit_activity')
        is_valid = self.is_response_valid(data)
        if not is_valid:
            logger.info(f'No commit history for repository {owner}/{repo}')
            return

        self.collection_refs['widgets'].document(self.get_repo_hash(owner, repo)).set({
            'commit_activity': data
        }, merge=True)

    def participation(self, owner, repo):
        # formatting will be in here
        data = self.actor.github_rest_make_request(
            f'/repos/{owner}/{repo}/stats/participation')
        is_valid = self.is_response_valid(data)
        if not is_valid:
            logger.info(f'No participation for repository {owner}/{repo}')
            return

        chart_data = {
            "xAxis": {"type": "category"},
            "yAxis": {"type": "value"},
            "series": []
        }

        # Subtract 'owner' from 'all' to get 'others'
        others = [all_count - owner_count for all_count,
                  owner_count in zip(data["all"], data["owner"])]

        chart_data["series"] = [
            {"name": "All", "data": data["all"], "type": "line"},
            {"name": "Owners", "data": data["owner"], "type": "line"},
            {"name": "Others", "data": others, "type": "line"}
        ]

        # Convert the chart data to JSON string
        # chart_data_json = json.dumps(chart_data)

        self.collection_refs['widgets'].document(self.get_repo_hash(owner, repo)).set({
            'participation': chart_data
        }, merge=True)

    def code_frequency(self, owner, repo):
        # formatting will be in here
        data = self.actor.github_rest_make_request(
            f'/repos/{owner}/{repo}/stats/code_frequency')
        is_valid = self.is_response_valid(data)
        if not is_valid:
            logger.info(f'No code frequency for repository {owner}/{repo}')
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
        # Convert the chart data to JSON string
        # chart_data_json = json.dumps(chart_data)

        self.collection_refs['widgets'].document(self.get_repo_hash(owner, repo)).set({
            'code_frequency': chart_data
        }, merge=True)

    def commit_participation(self, owner, repo):
        # formatting will be in frontend
        data = self.actor.github_rest_make_request(
            f'/repos/{owner}/{repo}/stats/commit_participation')
        is_valid = self.is_response_valid(data)
        if not is_valid:
            logger.info(
                f'No commit participation for repository {owner}/{repo}')
            return

        self.collection_refs['widgets'].document(self.get_repo_hash(owner, repo)).set({
            'commit_participation': data
        }, merge=True)

    def community_profile(self, owner, repo):
        # formatting will be in frontend
        data = self.actor.github_rest_make_request(
            f'/repos/{owner}/{repo}/community/profile')
        is_valid = self.is_response_valid(data)
        if not is_valid:
            logger.info(f'No community profile for repository {owner}/{repo}')
            return

        self.collection_refs['widgets'].document(self.get_repo_hash(owner, repo)).set({
            'community_profile': data
        }, merge=True)

    def punch_card(self, owner, repo):
        # formatting will be in frontend
        data = self.actor.github_rest_make_request(
            f'/repos/{owner}/{repo}/stats/punch_card')
        is_valid = self.is_response_valid(data)
        if not is_valid:
            logger.info(f'No punch card for repository {owner}/{repo}')
            return

        # Convert the chart data to JSON string
        # chart_data_json = json.dumps(data)

        self.collection_refs['widgets'].document(self.get_repo_hash(owner, repo)).set({
            'punch_card': data
        }, merge=True)

    def issue_count(self, owner, repo):
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
        is_valid = self.is_response_valid(data)
        if not is_valid:
            logger.info(f'No issues for repository {owner}/{repo}')
            return

        self.collection_refs['widgets'].document(self.get_repo_hash(owner, repo)).set({
            'issue_count': {
                'opened': data['data']['repository']['openedIssues']['totalCount'],
                'closed': data['data']['repository']['closedIssues']['totalCount']
            }
        }, merge=True)

    def issue_activity(self, owner, repo):
        # formatting will be in api
        query = """
            query ($owner: String!, $name: String!, $cursor: String) {
                repository(owner: $owner, name: $name) {
                    issues(first: 100, after: $cursor) {
                        pageInfo {
                            endCursor
                            hasNextPage
                        }
                        totalCount
                        nodes {
                            createdAt
                            closedAt
                            closed
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
                query, {'owner': owner, 'name': repo, 'cursor': cursor})
            is_valid = self.is_response_valid(result)
            if not is_valid:
                logger.info(f'No issue activity for repository {owner}/{repo}')
                break

            nodes = result["data"]["repository"]["issues"]["nodes"]
            end_cursor = result["data"]["repository"]["issues"]["pageInfo"]["endCursor"]
            has_next_page = result["data"]["repository"]["issues"]["pageInfo"]["hasNextPage"]

            # Extract relevant data points
            for node in nodes:
                created_at = node["createdAt"]
                closed_at = node["closedAt"]
                closed = node["closed"]
                issues.append({"createdAt": created_at,
                              "closedAt": closed_at, "closed": closed})

            # Set the next cursor for pagination
            cursor = end_cursor

        # Sort the data points by createdAt timestamp
        issues.sort(key=lambda x: datetime.strptime(
            x["createdAt"], "%Y-%m-%dT%H:%M:%SZ"))

        # Initialize empty lists for the x-axis (time) and y-axes (opened and closed issues)
        # time_axis = []
        # opened_issues = []
        # closed_issues = []

        # Calculate the average days to close an issue
        total_days = 0
        closed_issue_count = 0

        # Process the issues data
        for issue in issues:
            created_at = issue["createdAt"]
            closed_at = issue["closedAt"]
            closed = issue["closed"]

            # time_axis.append(created_at)

            # if closed:
            #     opened_issues.append(None)
            #     closed_issues.append(closed_at)
            # else:
            #     opened_issues.append(created_at)
            #     closed_issues.append(None)

            created_at_time = datetime.strptime(
                issue['createdAt'], "%Y-%m-%dT%H:%M:%SZ")
            
            if closed:
                closed_at_time = datetime.strptime(
                    issue['closedAt'], "%Y-%m-%dT%H:%M:%SZ")
                days_to_close = (closed_at_time - created_at_time).days
                total_days += days_to_close
                closed_issue_count += 1

        # Convert the data to Apache ECharts format
        # echarts_data = {
        #     "xAxis": {"data": time_axis},
        #     "yAxis": {},
        #     "series": [
        #         {"name": "Opened", "type": "line", "data": opened_issues},
        #         {"name": "Closed", "type": "line", "data": closed_issues}
        #     ]
        # }

        # # Convert the data to JSON
        # echarts_json = json.dumps(echarts_data)

        self.collection_refs['widgets'].document(self.get_repo_hash(owner, repo)).set({
            'issue_activity': issues
        }, merge=True)

        self.collection_refs['widgets'].document(self.get_repo_hash(owner, repo)).set({
            'average_days_to_close_issue': total_days / closed_issue_count
        }, merge=True)

    def most_active_issues(self, owner, repo, count: int = 10):
        # formatting will be in frontend

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
                query ($owner: String!, $name: String!, $since: DateTime!) {
                repository(owner: $owner, name: $name) {
                    issues(first: 10, orderBy: {field: COMMENTS, direction: DESC}, filterBy: {since: $since}) {
                    nodes {
                        number
                        author {
                        avatarUrl
                        login
                        }
                        title
                        state
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
                query, {'owner': owner, 'name': repo, 'since': since.isoformat()})
            is_valid = self.is_response_valid(data)
            if not is_valid:
                logger.info(f'No issues for repository {owner}/{repo}')
                return


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
                    'updated_at': issue['updatedAt']
                }
                flattened_data[interval].append(flattened_issue)

        self.collection_refs['widgets'].document(self.get_repo_hash(owner, repo)).set({
            'most_active_issues': flattened_data
        }, merge=True)

    def pull_request_count(self, owner, repo):
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
        is_valid = self.is_response_valid(data)
        if not is_valid:
            logger.info(f'No pull requests for repository {owner}/{repo}')
            return
        
        self.collection_refs['widgets'].document(self.get_repo_hash(owner, repo)).set({
            'pull_request_count': {
                'opened': data['data']['repository']['openedPullRequests']['totalCount'],
                'closed': data['data']['repository']['closedPullRequests']['totalCount']
            }
        }, merge=True)

    def pull_request_activity(self, owner, repo):
        # formatting will be in api
        query = """
        query ($owner: String!, $name: String!, $cursor: String) {
            repository(owner: $owner, name: $name) {
                pullRequests(first: 100, after: $cursor) {
                    pageInfo {
                        endCursor
                        hasNextPage
                    }
                    nodes {
                        createdAt
                        closedAt
                        closed
                    }
                }
            }
        }
        """
        cursor = None
        has_next_page = True
        pull_requests = []

        # Paginate through the results
        while has_next_page:
            # Make the request
            result = self.actor.github_graphql_make_query(
                query, {'owner': owner, 'name': repo, 'cursor': cursor})
            is_valid = self.is_response_valid(result)
            if not is_valid:
                logger.info(f'No pull requests for repository {owner}/{repo}')
                return

            # Extract the data
            nodes = result["data"]["repository"]["pullRequests"]["nodes"]
            end_cursor = result["data"]["repository"]["pullRequests"]["pageInfo"]["endCursor"]
            has_next_page = result["data"]["repository"]["pullRequests"]["pageInfo"]["hasNextPage"]

            # Extract relevant data points
            for node in nodes:
                created_at = node["createdAt"]
                closed_at = node["closedAt"]
                closed = node["closed"]
                pull_requests.append(
                    {"createdAt": created_at, "closedAt": closed_at, "closed": closed})

            # Set the next cursor for pagination
            cursor = end_cursor

        # Sort the data points by createdAt timestamp
        pull_requests.sort(key=lambda x: datetime.strptime(
            x["createdAt"], "%Y-%m-%dT%H:%M:%SZ"))

        # Initialize empty lists for the x-axis (time) and y-axes (opened and closed pull requests)
        # time_axis = []
        # opened_pull_requests = []
        # closed_pull_requests = []

        # Calculate the average days to close a pull request
        total_days = 0
        closed_pull_request_count = 0

        # Process the pull requests data
        for pull_request in pull_requests:
            created_at = pull_request["createdAt"]
            closed_at = pull_request["closedAt"]
            closed = pull_request["closed"]

            # time_axis.append(created_at)

            # if closed:
            #     opened_pull_requests.append(None)
            #     closed_pull_requests.append(closed_at)
            # else:
            #     opened_pull_requests.append(created_at)
            #     closed_pull_requests.append(None)

            created_at_time = datetime.strptime(
                pull_request['createdAt'], "%Y-%m-%dT%H:%M:%SZ")
            if closed:
                closed_at_time = datetime.strptime(
                    pull_request['closedAt'], "%Y-%m-%dT%H:%M:%SZ")
                days_to_close = (closed_at_time - created_at_time).days
                total_days += days_to_close
                closed_pull_request_count += 1

        # # Convert the data to Apache ECharts format
        # echarts_data = {
        #     "xAxis": {"data": time_axis},
        #     "yAxis": {},
        #     "series": [
        #         {"name": "Opened", "type": "line", "data": opened_pull_requests},
        #         {"name": "Closed", "type": "line", "data": closed_pull_requests}
        #     ]
        # }

        # # Convert the data to JSON
        # echarts_json = json.dumps(echarts_data)

        self.collection_refs['widgets'].document(self.get_repo_hash(owner, repo)).set({
            'pull_request_activity': pull_requests
        }, merge=True)

        self.collection_refs['widgets'].document(self.get_repo_hash(owner, repo)).set({
            'average_days_to_close_pull_request': total_days / closed_pull_request_count
        }, merge=True)

    def most_active_pull_requests(self, owner, repo):
        # formatting will be in frontend

        flattened_data = {}
        for interval in ['day', 'week', 'month', 'year']:
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
            query ($owner: String!, $name: String!, $since: DateTime!) {
            repository(owner: $owner, name: $name) {
                pullRequests(first: 100, orderBy: {field: COMMENTS, direction: DESC}, states: OPEN, filterBy: {since: $since}) {
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
                    createdAt
                    updatedAt
                }
                }
            }
            }
            """
            data = self.actor.github_graphql_make_query(
                query, {'owner': owner, 'name': repo, 'since': since.isoformat()})
            is_valid = self.is_response_valid(data)
            if not is_valid:
                logger.info(f'No pull requests for repository {owner}/{repo}')
                return

            # Extract the pull requests from the response
            pull_requests = data['repository']['pullRequests']['nodes']

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
                    'updated_at': pull_request['updatedAt']
                }
                flattened_data[interval].append(flattened_pull_request)

        self.collection_refs['widgets'].document(self.get_repo_hash(owner, repo)).set({
            'most_active_pull_requests': flattened_data
        }, merge=True)

    def language_breakdown(self, owner, repo):
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
        is_valid = self.is_response_valid(data)
        if not is_valid:
            logger.info(f'No languages for repository {owner}/{repo}')
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
                'percentage': percentage,
                'name': language['node']['name']
            }
            flattened_data.append(flattened_language)

        self.collection_refs['widgets'].document(self.get_repo_hash(owner, repo)).set({
            'language_breakdown': flattened_data
        }, merge=True)

    def recent_issues(self, owner, repo):
        # formatting will be in frontend
        query = """
        query ($owner: String!, $name: String!) {
            repository(owner: $owner, name: $name) {
                issues(first: 100, orderBy: {field: CREATED_AT, direction: DESC}) {
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
                        createdAt
                        updatedAt
                    }
                }
            }
        }
        """
        data = self.actor.github_graphql_make_query(
            query, {'owner': owner, 'name': repo})
        is_valid = self.is_response_valid(data)
        if not is_valid:
            logger.info(f'No issues for repository {owner}/{repo}')
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
                'updated_at': issue['updatedAt']
            }
            flattened_data.append(flattened_issue)

        self.collection_refs['widgets'].document(self.get_repo_hash(owner, repo)).set({
            'recent_issues': flattened_data
        }, merge=True)

    def recent_pull_requests(self, owner, repo):
        # formatting will be in frontend
        query = """
        query ($owner: String!, $name: String!) {
            repository(owner: $owner, name: $name) {
                pullRequests(first: 100, orderBy: {field: CREATED_AT, direction: DESC}) {
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
                        createdAt
                        updatedAt
                    }
                }
            }
        }
        """
        data = self.actor.github_graphql_make_query(
            query, {'owner': owner, 'name': repo})
        is_valid = self.is_response_valid(data)
        if not is_valid:
            logger.info(f'No pull requests for repository {owner}/{repo}')
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
                'updated_at': pull_request['updatedAt']
            }
            flattened_data.append(flattened_pull_request)

        self.collection_refs['widgets'].document(self.get_repo_hash(owner, repo)).set({
            'recent_pull_requests': flattened_data
        }, merge=True)

    def recent_stargazing_activity(self, owner, repo, max_fetch_pages=10):
        # formatting will be in here
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
            is_valid = self.is_response_valid(result)
            if not is_valid:
                logger.info(f'No pull requests for repository {owner}/{repo}')
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

        # Convert the dictionary to a Pandas DataFrame
        df = pd.DataFrame.from_dict(
            star_dict, orient='index', columns=['count'])
        df.index = pd.to_datetime(df.index)

        # Group the data by weeks or days depending on the range of dates
        date_range = df.index.max() - df.index.min()
        if date_range > timedelta(days=30):
            df = df.resample('W').sum()
        else:
            df = df.resample('D').sum()

        # Convert the DataFrame back to a dictionary
        flattened_data = df.to_dict()['count']

        total_stargazing_count = """
        query ($owner: String!, $name: String!) {
            repository(owner: $owner, name: $name) {
                stargazers {
                    totalCount
                }
            }
        }
        """

        total_stargazers = self.actor.github_graphql_make_query(
            total_stargazing_count, {'owner': owner, 'name': repo})
        is_valid = self.is_response_valid(total_stargazers)
        if not is_valid:
            logger.info(f'No pull requests for repository {owner}/{repo}')
            return

        
        self.collection_refs['widgets'].document(self.get_repo_hash(owner, repo)).set({
            'recent_stargazing_activity': flattened_data
        }, merge=True)

    def recent_commits(self, owner, repo):
        # formatting will be in frontend
        query = """
        query ($owner: String!, $name: String!) {
            repository(owner: $owner, name: $name) {
                defaultBranchRef {
                    target {
                        ... on Commit {
                            history(first: 100) {
                                nodes {
                                    author {
                                        avatarUrl
                                        user {
                                            login
                                        }
                                    }
									url
                                    message
                                    committedDate
                                }
                            }
                        }
                    }
                }
            }
        }
        """
        data = self.actor.github_graphql_make_query(
            query, {'owner': owner, 'name': repo})
        is_valid = self.is_response_valid(data)
        if not is_valid:
            logger.info(f'No commits for repository {owner}/{repo}')
            return

        flattened_data = []

        # Extract the commits from the response
        commits = data['data']['repository']['ref']['target']['history']['nodes']

        # Iterate through the commits and flatten the data
        for commit in commits:
            flattened_commit = {
                'author_avatar_url': commit['author']['avatarUrl'],
                'author_login': commit['author']['user']['login'],
                'message': commit['message'],
                'committed_date': commit['committedDate'],
                'url': commit['url']
            }
            flattened_data.append(flattened_commit)

        self.collection_refs['widgets'].document(self.get_repo_hash(owner, repo)).set({
            'recent_commits': flattened_data
        }, merge=True)

    def recent_releases(self, owner, repo):
        # formatting will be in frontend
        query = """
        query ($owner: String!, $name: String!) {
            repository(owner: $owner, name: $name) {
                releases(last: 10) {
                    nodes {
                        name
                        tagName
                        publishedAt
                        url
                    }
                }
            }
        }
        """
        data = self.actor.github_graphql_make_query(
            query, {'owner': owner, 'name': repo})
        is_valid = self.is_response_valid(data)
        if not is_valid:
            logger.info(f'No releases for repository {owner}/{repo}')
            return

        flattened_data = []

        # Extract the releases from the response
        releases = data['data']['repository']['releases']['nodes']

        # Iterate through the releases and flatten the data
        for release in releases:
            flattened_release = {
                'name': release['name'],
                'tag_name': release['tagName'],
                'published_at': release['publishedAt'],
                'url': release['url']
            }
            flattened_data.append(flattened_release)

        self.collection_refs['widgets'].document(self.get_repo_hash(owner, repo)).set({
            'recent_releases': flattened_data
        }, merge=True)
