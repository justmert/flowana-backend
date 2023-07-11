from github_actor import GithubActor
import logging
import log_config
import json
from datetime import datetime
from enum import Enum
import pandas as pd
from datetime import timedelta
from github_widgets import GithubWidgets
from github_cumulative import GithubCumulative
from discourse_widgets import DiscourseWidgets
from discourse_actor import DiscourseActor
logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, app, db, github_actor: GithubActor):
        self.app = app
        self.db = db
        self.repositories = []
        self.github_actor = github_actor
        self.project_pipeline_functions = []
        self.collection_refs = {}

    def contruct_pipeline(self, protocol, collection_refs):
        
        self.protocol_name = protocol['name']

        if protocol.get('forum', None):
            logger.info(
                f'Found forum configuration for protocol {self.protocol_name}.')
            forum_base_url = protocol['forum'][:-1] if protocol['forum'].endswith(
                '/') else protocol['forum']
            self.discourse_actor = DiscourseActor(forum_base_url)
            self.discourse_widgets = DiscourseWidgets(self.discourse_actor, collection_refs)
            self.protocol_discourse_functions = [
                self.discourse_widgets.categories
            ]


        self.collection_refs = collection_refs
        repositories_ref_stream = self.db.collection(
            f'{self.protocol_name}-projects').stream()

        self.repositories = []

        # Print the data for each document
        for repository in repositories_ref_stream:
            self.repositories.append(repository.to_dict())

        logger.info(
            f'Found {len(self.repositories)} repositories for protocol {self.protocol_name} to be updated.')

        self.github_widgets = GithubWidgets(self.github_actor, collection_refs)
        self.github_cumulative = GithubCumulative(self.github_actor, collection_refs)

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

        self.protocol_github_functions = [
            self.github_cumulative.cumulative_stats,
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


    def run_pipelines(self):
        # self.run_project_github_pipeline()
        # self.run_protocol_pipeline(self.protocol_github_functions)
        print('---')
        self.run_protocol_pipeline(self.protocol_discourse_functions)
        print('2---')




    def run_protocol_pipeline(self, protocol_pipeline):
        for item in protocol_pipeline:
            if isinstance(item, tuple) or isinstance(item, list):
                logging.info(
                    f'[*] Running cumulative github function: {item[0].__name__}')
                if len(item) == 1:
                    func = item[0]
                    func()
                else:
                    func, args = item
                    func(**args)
            else:
                logging.info(
                    f'[*] Running cumulative github function: {item.__name__}')
                item()

    def run_project_github_pipeline(self):
        # for repository in self.repositories:
        for repository in [
            {'owner': 'justmert', 'repo': 'test'}
        ]:

            is_valid = self.github_actor.check_repo_validity(
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

