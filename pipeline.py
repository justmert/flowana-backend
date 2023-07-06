from request_actor import Actor
import logging
import log_config
import json
from datetime import datetime
from enum import Enum
import pandas as pd
from datetime import timedelta
from github_widgets import GithubWidgets
from github_cumulative import GithubCumulative
logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, app, db, actor: Actor):
        self.app = app
        self.db = db
        self.actor = actor
        self.repositories = []
        self.project_pipeline_functions = []
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

        self.github_widgets = GithubWidgets(self.actor, collection_refs)
        self.github_cumulative = GithubCumulative(self.actor, collection_refs)

        self.project_pipeline_functions = [

            self.github_widgets.repository_info,
            self.github_widgets.commit_activity,
            self.github_widgets.code_frequency,
            self.github_widgets.participation,
            self.github_widgets.code_frequency,
            self.github_widgets.community_profile,
            self.github_widgets.punch_card,

            self.github_widgets.issue_count,
            (self.github_widgets.recent_issues, {'order_by': 'CREATED_AT'}),
            (self.github_widgets.recent_issues, {'order_by': 'UPDATED_AT'}),
            self.github_widgets.most_active_issues,


            self.github_widgets.pull_request_count,
            (self.github_widgets.recent_pull_requests,
             {'order_by': 'CREATED_AT'}),
            (self.github_widgets.recent_pull_requests,
             {'order_by': 'UPDATED_AT'}),

            self.github_widgets.language_breakdown,
            self.github_widgets.recent_stargazing_activity,
            self.github_widgets.recent_commits,
            self.github_widgets.recent_releases,
        ]

        self.cumulative_functions = [
            self.github_cumulative.cumulative_repository_stats,
            self.github_cumulative.cumulative_commit_activity,
            self.github_cumulative.cumulative_code_frequency,
            self.github_cumulative.cumulative_participation,
            self.github_cumulative.cumulative_punch_card,

            self.github_cumulative.cumulative_issue_count,
            (self.github_cumulative.cumulative_recent_issues,
             {'order_by': 'CREATED_AT'}),
            (self.github_cumulative.cumulative_recent_issues,
             {'order_by': 'UPDATED_AT'}),
            self.github_cumulative.cumulative_most_active_issues,

            self.github_cumulative.cumulative_pull_request_count,
            (self.github_cumulative.cumulative_recent_pull_requests,
             {'order_by': 'CREATED_AT'}),
            (self.github_cumulative.cumulative_recent_pull_requests,
             {'order_by': 'UPDATED_AT'}),

            self.github_cumulative.cumulative_language_breakdown,
            self.github_cumulative.cumulative_recent_commits,
            self.github_cumulative.cumulative_recent_releases
        ]

    def run_pipeline(self):
        # for repository in self.repositories:
        for repository in [
            #     {'owner': 'lensterxyz', 'repo': 'lenster'},
            #    {'owner': 'lenstube-xyz', 'repo': 'lenstube'},
            #    {'owner': 'lens-protocol', 'repo': 'core'},
            #    {'owner': 'SendACoin', 'repo': 'sendacoin.to'},
            {'owner': 'justmert', 'repo': 'test'}
        ]:

            is_valid = self.actor.check_repo_validity(
                repository['owner'], repository['repo'])
            if not is_valid:
                logger.info(
                    f'Repository {repository["owner"]}/{repository["repo"]} is not valid.')
                continue

            doc_ref = self.collection_refs['widgets'].document(
                f"{repository['owner']}#{repository['repo']}")
            doc = doc_ref.get()
            if not doc.exists:
                logger.info(
                    f'Repository {repository["owner"]}/{repository["repo"]} does not exist in database. Creating it now.')
                doc_ref.set({})

            logger.info(
                f'Running pipeline for repository {repository["owner"]}/{repository["repo"]}')

            for item in self.project_pipeline_functions:
                if isinstance(item, tuple) or isinstance(item, list):
                    logging.info(
                        f'[*] Running pipeline function: {item[0].__name__}')
                    if len(item) == 1:
                        func = item[0]
                        func(repository['owner'], repository['repo'])
                    else:
                        func, args = item
                        func(repository['owner'], repository['repo'], **args)
                else:
                    logging.info(
                        f'[*] Running pipeline function: {item.__name__}')
                    item(repository['owner'], repository['repo'])

            logger.info(
                f'Finished running pipeline for repository {repository["owner"]}/{repository["repo"]}')

        for item in self.cumulative_functions:
            if isinstance(item, tuple) or isinstance(item, list):
                logging.info(
                    f'[*] Running cumulative function: {item[0].__name__}')
                if len(item) == 1:
                    func = item[0]
                    func()
                else:
                    func, args = item
                    func(**args)
            else:
                logging.info(
                    f'[*] Running cumulative function: {item.__name__}')
                item()
