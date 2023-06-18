import json
from request_actor import Actor
from datetime import datetime
# import pandas as pd
# from collections import defaultdict
from datetime import datetime, timedelta
import operator
# from dotenv import load_dotenv
import os


class Update:

    # The Update class is responsible for updating the Firestore database with information about GitHub repositories. It uses the GRequest class to make requests to the GitHub API and fetches data about various aspects of the repositories, such as commit history, code frequency, issue activity, and top contributors. The class then writes this data to the Firestore database, organized by repository and ecosystem.

    def __init__(self, ecosystem, fire_ctx=None):

        # - make_request: An instance of the GRequest class for making requests to the GitHub API.
        # - data: A dictionary containing data about the repositories.
        # - collection_index: An index used for organizing data in the Firestore database.
        # - fire_app: An instance of the FirestoreDB class for accessing the Firestore database.
        # - db: A reference to the Firestore database.
        # - info_collection_ref: A reference to the collection in the Firestore database containing information about the repositories.
        # - data_collection_ref: A reference to the collection in the Firestore database containing data about the repositories.
        # - overall_collection_ref: A reference to the collection in the Firestore database containing overall data about the ecosystem.
        # - all_repo_names: A list of all repository names in the ecosystem.

        self.make_request = Actor()
        self.data = {}
        self.collection_index = 0

        if fire_ctx is not None:
            self.fire_app: FirestoreDB = fire_ctx.get_app
            self.db: FirestoreDB = fire_ctx.get_db

            self.info_collection_ref = self.db.collection(
                f'{ecosystem}-repositories-info')
            self.data_collection_ref = self.db.collection(
                f'{ecosystem}-repositories-data')
            self.overall_collection_ref = self.db.collection(
                f'{ecosystem}-repositories-overall')

            self.all_repo_names = self.data_collection_ref.document(
                u'_names').get().to_dict()["repository_names"]

    # - seed(): Seeds the database with information about all repositories in the ecosystem.
    # - init_overall(): Initializes the overall_data dictionary with default values for various metrics.
    # - write_overall(): Writes the overall_data dictionary to the Firestore database.
    # - overall_total_project_count(): Calculates the total number of projects in the ecosystem and updates the overall_data dictionary.
    # - overall_star_count(): Calculates the total number of stars in the ecosystem and updates the overall_data dictionary.
    # - overall_top_contributors(): Calculates the top contributors in the ecosystem and updates the overall_data dictionary.
    # - overall_recent_commits(): Calculates the most recent commits in the ecosystem and updates the overall_data dictionary.
    # - overall_pull_request_count(): Calculates the number of open and closed pull requests in the ecosystem and updates the overall_data dictionary.
    # - overall_issue_count(): Calculates the number of open and closed issues in the ecosystem and updates the overall_data dictionary.
    # - overall_issue_activity(): Calculates the issue activity in the ecosystem and updates the overall_data dictionary.
    # - overall_code_frequency(): Calculates the code frequency in the ecosystem and updates the overall_data dictionary.
    # - overall_commit_history(): Calculates the commit history in the ecosystem and updates the overall_data dictionary.
    # - fetch_and_write(): Fetches data about a specific repository and writes it to the Firestore database.
    # - get_data(): Getter method for the data field.
    # - _get_hash(): Helper method for generating a hash value for a repository.
    # - repository_info(): Fetches information about a repository.
    # - commit_history(): Fetches the commit history for a repository.
    # - code_frequency(): Fetches the code frequency for a repository.
    # - issue_activity(): Fetches the issue activity for a repository.
    # - issue_count(): Fetches the issue count for a repository.
    # - pull_request_count(): Fetches the pull request count for a repository.
    # - pull_request_activity(): Fetches the pull request activity for a repository.
    # - pull_request_activity_ql(): Fetches the pull request activity for a repository using GraphQL.
    # - star_activity(): Fetches the star activity for a repository.
    # - top_contributors(): Fetches the top contributors for a repository.
    # - recent_commits(): Fetches the most recent commits for a repository.

    def seed(self):
        print('[*] Seeding the database')
        self.init_overall()
        # self.all_repo_info_doc_ref = self.info_collection_ref.document(u'_info')
        for repo in self.all_repo_names:
            print(f'[*] {repo}')
            valid = self.make_request.check_repo_validity(repo['owner'], repo['repo'])
            if valid:
                self.fetch_and_write(repo)

        self.overall_data['total_project_count'] = len(self.all_repo_names)
        self.write_overall()

    def init_overall(self):
        self.overall_data = {
            "commit_history": {
                "xAxis": {"data": None},
                "yAxis": {},
                "series": [
                    {"data": [], "type": "line"}
                ],
            },
            # data that should be edited
            "_code_frequency": {},
            "_issue_activity": {},
            "recent_issues": [],
            "recent_commits": [],
            "issue_count": {
                "series": [
                    {
                        "data": [
                            {"value": 0, "name": 'Open'},
                            {"value": 0, "name": 'Closed'},
                        ]
                    }
                ]
            },
            "pull_request_count": {
                "series": [
                    {
                        "data": [
                            {"value": 0, "name": 'Open'},
                            {"value": 0, "name": 'Closed'},
                        ]
                    }
                ]
            },
            "top_contributors": {},
            "total_project_count": 0,
            "total_star_count": 0
        }

    def write_overall(self):

        # ----------------- commit history -----------------
        # if self.overall_data["commit_history"]:
        self.overall_commit_history()

        # ----------------- code frequency -----------------
        self.overall_code_frequency()

        # ----------------- issue activity -----------------
        self.overall_issue_activity()

        # ----------------- issue count -----------------
        self.overall_issue_count()

        # ----------------- pull request count -----------------
        self.overall_pull_request_count()

        # ----------------- recent commits -----------------
        self.overall_recent_commits()

        # ----------------- top contributors -----------------
        self.overall_top_contributors()

        # ----------------- total project count -----------------
        self.overall_total_project_count()

        # ----------------- total star count -----------------
        self.overall_star_count()

    def overall_total_project_count(self):
        self.overall_collection_ref.document(u'_overall').set({
            u'total_project_count': self.overall_data["total_project_count"],
        }, merge=True)

    def overall_star_count(self):
        self.overall_collection_ref.document(u'_overall').set({
            u'total_star_count': self.overall_data["total_star_count"],
        }, merge=True)

    def overall_top_contributors(self):
        if (len(self.overall_data["top_contributors"]) == 0):
            option = None
        else:
            contributors = {k: v for k, v in sorted(self.overall_data["top_contributors"].items(
            ), key=lambda item: item[1]["contributions"], reverse=True)}

            option = list(contributors.values())[:4]

        self.overall_collection_ref.document(u'_overall').set({
            u'top_contributors': option,
        }, merge=True)

    def overall_recent_commits(self):
        if self.overall_data["recent_commits"]:
            option = self.overall_data["recent_commits"]
        else:
            option = None
        self.overall_collection_ref.document(u'_overall').set({
            u'recent_commits': option,
        }, merge=True)

    def overall_pull_request_count(self):
        if self.overall_data["pull_request_count"]["series"][0]["data"][0]["value"] == 0 and self.overall_data["pull_request_count"]["series"][0]["data"][1]["value"] == 0:
            option = None

        else:
            option = self.overall_data["pull_request_count"]

        self.overall_collection_ref.document(u'_overall').set({
            u'pull_request_count': option,
        }, merge=True)

    def overall_issue_count(self):
        if self.overall_data["issue_count"]["series"][0]["data"][0]["value"] == 0 and self.overall_data["issue_count"]["series"][0]["data"][1]["value"] == 0:
            option = None

        else:
            option = self.overall_data["issue_count"]
        self.overall_collection_ref.document(u'_overall').set({
            u'issue_count': option,
        }, merge=True)

    def overall_issue_activity(self):
        zipped = {str(k): v for k, v in sorted(
            self.overall_data["_issue_activity"].items(), key=lambda item: item[0], reverse=False)}

        y_open = [v['open'] for _, v in zipped.items()]
        y_closed = [v['closed'] for _, v in zipped.items()]
        if len(y_open) == 0 and len(y_closed) == 0:
            option = None
            self.overall_data["recent_issues"] = None

        else:
            option = {
                "xAxis": {
                    "data": list(zipped.keys())
                },
                "yAxis": {},
                "series": [
                    {
                        "data": y_open,
                        "type": "bar",
                        "stack": "issue_activity",
                        "name": "open"
                    },
                    {
                        "data": y_closed,
                        "type": "bar",
                        "stack": "issue_activity",
                        "name": "closed"
                    }
                ],
            }

        self.overall_collection_ref.document(u'_overall').set({
            u'issue_activity': option,
        }, merge=True)

        self.overall_collection_ref.document(u'_overall').set({
            u'recent_issues': self.overall_data["recent_issues"],
        }, merge=True)

        del self.overall_data["_issue_activity"]

    def overall_code_frequency(self):
        if (len(self.overall_data["_code_frequency"]) == 0):
            option = None

        else:
            # Initialize the series data
            series = [
                {"data": [x[0] for x in self.overall_data["_code_frequency"].values(
                )], "type": "line", "stack": "x"},
                {"data": [x[1] for x in self.overall_data["_code_frequency"].values(
                )], "type": "line", "stack": "x"}
            ]
            option = {
                "xAxis": {"data": list(self.overall_data["_code_frequency"].keys())},
                "yAxis": {},
                "series": series,
            }
            del self.overall_data["_code_frequency"]

        self.overall_collection_ref.document(u'_overall').set({
            u'code_frequency': option,
        }, merge=True)

    def overall_commit_history(self):
        if (len(self.overall_data["commit_history"]["series"][0]["data"]) == 0):
            option = None

        else:
            option = self.overall_data["commit_history"]
        self.overall_collection_ref.document(u'_overall').set({
            u'commit_history': option,
        }, merge=True)

    def fetch_and_write(self, repo):
        try:
            data_repo_doc_ref = self.data_collection_ref.document(
                f"{repo['owner']}-{repo['repo']}")
            self.info_collection_ref.document(f"{repo['owner']}-{repo['repo']}").set({
                u'{}'.format("info"): self.repository_info(repo['owner'], repo['repo'])
            }, merge=True)
        except Exception as ex:
            print(ex)

        try:
            data_repo_doc_ref.set({
                u'commit_history': self.commit_history(repo['owner'], repo['repo'])
            }, merge=True)
        except Exception as ex:
            print(ex)

        try:
            data_repo_doc_ref.set({
                u'code_frequency': self.code_frequency(repo['owner'], repo['repo'])
            }, merge=True)
        except Exception as ex:
            print(ex)

        try:
            recent_issues, issue_activity = self.issue_activity(
                repo['owner'], repo['repo'])
            data_repo_doc_ref.set({
                u'recent_issues': recent_issues
            }, merge=True)
        except Exception as ex:
            print(ex)

        try:
            data_repo_doc_ref.set({
                u'issue_activity': issue_activity
            }, merge=True)

        except Exception as ex:
            print(ex)

        try:
            data_repo_doc_ref.set({
                u'issue_count': self.issue_count(repo['owner'], repo['repo'])
            }, merge=True)
        except Exception as ex:
            print(ex)

        try:
            data_repo_doc_ref.set({
                u'pull_request_count': self.pull_request_count(repo['owner'], repo['repo'])
            }, merge=True)    
        except Exception as ex:
            print(ex)

        try:
            data_repo_doc_ref.set({
                u'pull_request_activity': self.pull_request_activity(repo['owner'], repo['repo'])
            }, merge=True)
        except Exception as ex:
            print(ex)

        try:
            data_repo_doc_ref.set({
                u'star_activity': self.star_activity(repo['owner'], repo['repo'])
            }, merge=True)
        except Exception as ex:
            print(ex)
        try:
            data_repo_doc_ref.set({
                u'top_contributors': self.top_contributors(repo['owner'], repo['repo'])
            }, merge=True)
        except Exception as ex:
            print(ex)
        try:
            data_repo_doc_ref.set({
                u'recent_commits': self.recent_commits(repo['owner'], repo['repo'])
            }, merge=True)
        except Exception as ex:
            print(ex)

    @property
    def get_data(self):
        return self.data

    def _get_hash(self, owner, repo):
        return f"{owner}/{repo}"

    def repository_info(self, owner, repo):
        data = self.make_request.github_rest_make_request(
            f"/repos/{owner}/{repo}", max_page_fetch=1)

        if data is None:
            return None
        self.overall_data["total_star_count"] += data["stargazers_count"]

        return data

    def commit_history(self, owner, repo):
        # Returns the total commit counts for the owner and total commit counts in all. all is everyone combined, including the owner in the last 52 weeks
        data = self.make_request.github_rest_make_request(
            f"/repos/{owner}/{repo}/stats/participation")

        if data is None:
            return None
        # Initialize the series data
        series = [
            {"data": data["all"], "type": "line"},
        ]

        # Define the chart option
        x_range = list(range(len(data["all"])))
        option = {
            "xAxis": {"data": x_range},
            "yAxis": {},
            "series": series,
        }

        # overall calculation
        self.overall_data["commit_history"]["xAxis"]["data"] = x_range
        if self.overall_data["commit_history"]["series"][0]["data"] == []:
            self.overall_data["commit_history"]["series"][0]["data"] = data["all"]
        else:
            self.overall_data["commit_history"]["series"][0]["data"] = list(map(
                operator.add, self.overall_data["commit_history"]["series"][0]["data"], data["all"]))

        # self.data[self._get_hash(owner, repo)]['commit_history'] = option
        return option

    def code_frequency(self, owner, repo):
        # weekly aggregate of the number of additions and deletions pushed to a repository.
        data = self.make_request.github_rest_make_request(
            f"/repos/{owner}/{repo}/stats/code_frequency")

        if data is None:
            return None

        # Initialize the x-axis labels
        x_axis = []

        # Initialize the series data
        series = [
            {"data": [], "type": "line", "stack": "x"},
            {"data": [], "type": "line", "stack": "x"}
        ]

        # Iterate over the code frequency data
        for item in data:
            # Append the x-axis label
            _date = str(datetime.fromtimestamp(item[0]).date())
            x_axis.append(_date)
            is_date_exist = self.overall_data["_code_frequency"].get(
                _date, None)
            if is_date_exist is None:
                self.overall_data["_code_frequency"][_date] = [
                    0, 0]  # additions, deletions

            # Append the series data
            series[0]["data"].append(item[1])
            series[1]["data"].append(item[2])

            self.overall_data["_code_frequency"][_date][0] += item[1]
            self.overall_data["_code_frequency"][_date][1] += item[2]

        # Define the chart option
        option = {
            "xAxis": {"data": x_axis},
            "yAxis": {},
            "series": series,
        }

        return option

    def issue_activity(self, owner, repo):
        data = self.make_request.github_rest_make_request(f"/repos/{owner}/{repo}/issues",
                                                          {"state": "all", "per_page": 100, "sort": "updated", "direction": "desc"}, max_page_fetch=3)

        if data is None:
            return None, None

        # Initialize lists to store the data for the x and y axis
        # x_data = []
        y_data_state = {}

        recent_issues = []
        # Iterate through the issues
        for issue in data:
            is_pull_request = issue.get("pull_request", None)
            if is_pull_request is not None:  # meh, it is pr, skip it
                continue

            # Extract the date when the issues was updated
            updated_at = datetime.strptime(
                issue["updated_at"], "%Y-%m-%dT%H:%M:%SZ").date()

            # Append the date to the x data list
            # x_data.append(updated_at)
            updated_date_exist = y_data_state.get(updated_at, None)
            if updated_date_exist is None:
                y_data_state[updated_at] = {"open": 0, "closed": 0}

            updated_overall_exist = self.overall_data["_issue_activity"].get(
                updated_at, None)
            if updated_overall_exist is None:
                self.overall_data["_issue_activity"][updated_at] = {
                    "open": 0, "closed": 0}

            # Check if the issues is open or closed
            if issue["state"] == "open":
                # Increment the count for open issues
                y_data_state[updated_at]["open"] += 1
                self.overall_data["_issue_activity"][updated_at]["open"] += 1
            else:
                # Increment the count for closed issues
                y_data_state[updated_at]["closed"] += 1
                self.overall_data["_issue_activity"][updated_at]["closed"] += 1

            issue["created_at"] = str(issue["created_at"])
            issue["updated_at"] = str(issue["updated_at"])

            if len(recent_issues) < 5:
                recent_issues.append(issue)

        for _recent_issue in recent_issues:
            _inserted = False
            for overall_i, overall_val in enumerate(self.overall_data["recent_issues"]):
                if overall_val["updated_at"] < _recent_issue["updated_at"]:
                    self.overall_data["recent_issues"].insert(
                        overall_i, _recent_issue)
                    _inserted = True
                    break

            if not _inserted:
                self.overall_data["recent_issues"].append(_recent_issue)

        self.overall_data["recent_issues"] = self.overall_data["recent_issues"][:5]

        # print(y_data_state)
        # zipped = zip(y_data_state.keys(), y_data_state.values())
        # zipped = sorted(zipped, key=lambda x: x[0])
        zipped = {str(k): v for k, v in sorted(
            y_data_state.items(), key=lambda item: item[0], reverse=False)}

        y_open = [v['open'] for _, v in zipped.items()]
        y_closed = [v['closed'] for _, v in zipped.items()]
        if len(y_open) == 0 and len(y_closed) == 0:
            return None, None

        # Define the option for the stacked bar chart
        option = {
            "xAxis": {
                "data": list(zipped.keys())
            },
            "yAxis": {},
            "series": [
                {
                    "data": y_open,
                    "type": "bar",
                    "stack": "issue_activity",
                    "name": "open"
                },
                {
                    "data": y_closed,
                    "type": "bar",
                    "stack": "issue_activity",
                    "name": "closed"
                }
            ],
        }
        return recent_issues, option

    def issue_count(self, owner, repo):
        query = """
            query ($owner: String!, $name: String!) {
            repository(owner: $owner, name: $name) {
                    open: issues(states:OPEN) {
						totalCount
					}
					closed: issues(states:CLOSED) {
						totalCount    
                    }
                }
            }        
        """

        variables = {
            "owner": owner,
            "name": repo,
        }

        data = self.make_request.github_graphql_make_query(query, variables)

        if data is None:
            return None
        try:
            open_count = data["data"]["repository"]["open"]["totalCount"]
            closed_count = data["data"]["repository"]["closed"]["totalCount"]
            if open_count == 0 and closed_count == 0:
                return None

            option = {
                "series": [
                    {
                        "data": [
                            {"value": open_count, "name": 'Open'},
                            {"value": closed_count, "name": 'Closed'},
                        ]
                    }
                ]
            }

            # sum current overall count with new data
            self.overall_data["issue_count"]["series"][0]["data"][0]["value"] += data["data"]["repository"]["open"]["totalCount"]
            self.overall_data["issue_count"]["series"][0]["data"][1]["value"] += data["data"]["repository"]["closed"]["totalCount"]
        except Exception as ex:
            print(ex)
            return None
        return option

        # Initialize the pull request data

    def pull_request_count(self, owner, repo):
        query = """
            query ($owner: String!, $name: String!) {
            repository(owner: $owner, name: $name) {
                    open: pullRequests(states:OPEN) {
						totalCount
					}
					closed: pullRequests(states:CLOSED) {
						totalCount    
                    }
                }
            }        
        """

        variables = {
            "owner": owner,
            "name": repo,
        }

        data = self.make_request.github_graphql_make_query(query, variables)

        if data is None:
            return None

        open_count = data["data"]["repository"]["open"]["totalCount"]
        closed_count = data["data"]["repository"]["closed"]["totalCount"]
        option = {
            "series": [
                {
                    "data": [
                        {"value": open_count, "name": 'Open'},
                        {"value": closed_count, "name": 'Closed'},
                    ]
                }
            ]
        }

        # sum current overall count with new data
        self.overall_data["pull_request_count"]["series"][0]["data"][0]["value"] += data["data"]["repository"]["open"]["totalCount"]
        self.overall_data["pull_request_count"]["series"][0]["data"][1]["value"] += data["data"]["repository"]["closed"]["totalCount"]

        # self.data[self._get_hash(owner, repo)]['pull_request_count'] = option
        return option

    def pull_request_activity(self, owner, repo):
        data = self.make_request.github_rest_make_request(f"/repos/{owner}/{repo}/pulls",
                                                          {"state": "all", "per_page": 100, "sort": "updated", "direction": "desc"}, max_page_fetch=3)

        if data is None:
            return None

        # Initialize lists to store the data for the x and y axis
        # x_data = []
        y_data_state = {}

        # Iterate through the pull requests
        for pull_request in data:
            # Extract the date when the pull request was created

            # Extract the date when the pull request was updated
            updated_at = datetime.strptime(
                pull_request["updated_at"], "%Y-%m-%dT%H:%M:%SZ").date()

            # Append the date to the x data list
            # x_data.append(updated_at)
            updated_date_exist = y_data_state.get(updated_at, None)
            if updated_date_exist is None:
                y_data_state[updated_at] = {"open": 0, "closed": 0}

            # Check if the pull request is open or closed
            if pull_request["state"] == "open":
                # Increment the count for open pull requests
                y_data_state[updated_at]["open"] += 1
            else:
                # Increment the count for closed pull requests
                y_data_state[updated_at]["closed"] += 1
        zipped = {str(k): v for k, v in sorted(
            y_data_state.items(), key=lambda item: item[0], reverse=False)}

        y_open = [v['open'] for _, v in zipped.items()]
        y_closed = [v['closed'] for _, v in zipped.items()]
        if len(y_open) == 0 and len(y_closed) == 0:
            return None

        # Define the option for the stacked bar chart
        option = {
            "xAxis": {
                "data": list(zipped.keys())
            },
            "yAxis": {},
            "series": [
                {
                    "data": y_open,
                    "type": "bar",
                    "stack": "pull_requests",
                    "name": "open"
                },
                {
                    "data": y_closed,
                    "type": "bar",
                    "stack": "pull_requests",
                    "name": "closed"
                }
            ],
        }
        return option

    def pull_request_activity_ql(self, owner, repo):
        # Returns the number of pull requests opened and closed in the repository over the 12 months prior to the current date.
        now = datetime.now()
        one_year_ago = (now - timedelta(days=30)).date()

        query = """
            query ($owner: String!, $name: String!, $cursor: String) {
            repository(owner: $owner, name: $name) {
                pullRequests(first: 100, after: $cursor, states: [OPEN,CLOSED], orderBy: {field: UPDATED_AT, direction: DESC}) {
                nodes {
                    state
                    updatedAt
                }
                pageInfo {
                    endCursor
                    hasNextPage
                    }
                }
                }
            }        
        """

        variables = {
            "owner": owner,
            "name": repo,
            "cursor": None,
            # "query_date": one_year_ago.isoformat()
        }

        # Initialize the pull request data
        pull_requests = []
        while True:
            data = self.make_request.github_graphql_make_query(
                query, variables)
            # data = json.loads(res.text)
            latest_dt = datetime.strptime(
                data["data"]["repository"]["pullRequests"]["nodes"][-1]["updatedAt"], "%Y-%m-%dT%H:%M:%SZ").date()
            pull_requests.extend(
                data["data"]["repository"]["pullRequests"]["nodes"])
            if not data["data"]["repository"]["pullRequests"]["pageInfo"]["hasNextPage"]:
                break
            if latest_dt < one_year_ago:
                break
            variables["cursor"] = data["data"]["repository"]["pullRequests"]["pageInfo"]["endCursor"]

        combined = {}
        for pull_request in pull_requests:
            date = datetime.strptime(
                pull_request["updatedAt"], "%Y-%m-%dT%H:%M:%SZ").date()
            state = pull_request["state"]
            is_combined = combined.get(date, None)
            if is_combined is None:
                combined[date] = {"OPEN": 0, "CLOSED": 0}
            combined[date][state] += 1

        x_axis = list(sorted(combined.keys()))
        y_axis = {'open': [], 'closed': []}

        for date in x_axis:
            y_axis['open'].append(combined[date]['OPEN'])
            y_axis['closed'].append(combined[date]['CLOSED'])

        option = {
            "xAxis": {
                "type": 'category',
                "data": x_axis
            },
            "yAxis": {
                "type": 'value'
            },
            "series": [
                {
                    "data": y_axis['open'],
                    "type": 'bar',
                    "stack": 'pull_request',
                    "name": 'Open'
                },
                {
                    "data": y_axis['closed'],
                    "type": 'bar',
                    "stack": 'pull_request',
                    "name": 'Closed'
                }
            ],
        }
        return option

    def star_activity(self, owner, repo):
        query = """
            query ($owner: String!, $name: String!, $cursor: String) {
                repository(owner: $owner, name: $name) {
                    stargazers(first: 100, after: $cursor, orderBy: {field: STARRED_AT, direction: DESC}) {
                    pageInfo {
                        endCursor
                        hasNextPage
                    }
                    edges {
                        starredAt
                    }
                }
            }
            }
        """
        variables = {
            "owner": owner,
            "name": repo,
            "cursor": None
        }

        # Fetch all pages
        results = {}
        max_fetch_count = 6
        current_fetch = 0
        while current_fetch <= max_fetch_count:
            data = self.make_request.github_graphql_make_query(
                query, variables)

            # print(current_fetch)
            current_fetch += 1

            if data is None:
                return None

            # Extract the results
            for edge in data["data"]["repository"]["stargazers"]["edges"]:
                date_str = datetime.strptime(
                    edge["starredAt"], "%Y-%m-%dT%H:%M:%SZ").date()
                is_date_exist = results.get(date_str, None)
                # print(date_str)

                if is_date_exist is None:
                    results[date_str] = 0
                results[date_str] += 1

            # Check if there are more pages
            if data["data"]["repository"]["stargazers"]["pageInfo"]["hasNextPage"]:
                variables["cursor"] = data["data"]["repository"]["stargazers"]["pageInfo"]["endCursor"]
            else:
                break

        if(len(results) == 0):
            return None

        # zipped = zip(results.keys(), results.values())
        # zipped = sorted(zipped, key=lambda x: x[0])
        zipped = {str(k): v for k, v in sorted(
            results.items(), key=lambda item: item[0], reverse=False)}

        # print([k for k, _ in zipped])
        # print([v for _, v in zipped])

        option = {
            "xAxis": {
                "type": "category",
                "data": list(zipped.keys())
            },
            "yAxis": {
                "type": "value"
            },
            "series": [
                {
                    "data": list(zipped.values()),
                    "type": "line",
                }
            ]
        }
        return option

    def top_contributors(self, owner, repo):
        # Initialize variables to track contributors and their commit count
        contributors = {}

        # Fetch all contributors from API
        # page = 1
        variables = {
            "per_page": 100
        }
        # Send GET request to API endpoint
        data = self.make_request.github_rest_make_request(
            f"/repos/{owner}/{repo}/contributors", variables=variables, max_page_fetch=1)

        if data is None:
            return None

        # Add contributors and their commit count to dictionary
        for user in data:
            contributor_overall_exist = self.overall_data["top_contributors"].get(
                user['login'], None)
            if contributor_overall_exist is None:
                self.overall_data["top_contributors"][user['login']] = {
                    "login": user['login'],
                    "avatar_url": user['avatar_url'],
                    "html_url": user['html_url'],
                    "projects": [{
                        "name": repo,
                        "owner": owner,
                        "contributions": user['contributions']
                    }],
                    "contributions": user['contributions']
                }
            else:
                self.overall_data["top_contributors"][user['login']
                                                      ]["contributions"] += user['contributions']
                self.overall_data["top_contributors"][user['login']]['projects'].append({
                    "name": repo,
                    "owner": owner,
                    "contributions": user['contributions']
                })

        return data[:4]

    def recent_commits(self, owner, repo):
        variables = {
            "per_page": 5
        }
        data = self.make_request.github_rest_make_request(
            f"/repos/{owner}/{repo}/commits", variables=variables, max_page_fetch=1)

        if data is None:
            return None

        for _recent_commit in data:
            _inserted = False
            for overall_i, overall_val in enumerate(self.overall_data["recent_commits"]):
                if overall_val['commit']['author']["date"] < _recent_commit['commit']['author']["date"]:
                    self.overall_data["recent_commits"].insert(
                        overall_i, _recent_commit)
                    _inserted = True
                    break

            if not _inserted:
                self.overall_data["recent_commits"].append(_recent_commit)

        self.overall_data["recent_commits"] = self.overall_data["recent_commits"][:5]
        return data
