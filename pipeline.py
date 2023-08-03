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
from developers_widgets import DevelopersWidget
from developers_actor import DevelopersActor
from governance_actor import GovernanceActor
from governance_widgets import GovernanceWidgets
from messari_actor import MessariActor
from messari_widgets import MessariWidgets

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
        self.protocol_name = protocol["name"]

        if protocol.get("forum", None):
            logger.info(
                f"Found forum configuration for protocol {self.protocol_name}."
            )
            forum_base_url = (
                protocol["forum"][:-1]
                if protocol["forum"].endswith("/")
                else protocol["forum"]
            )
            self.discourse_actor = DiscourseActor(forum_base_url)
            self.discourse_widgets = DiscourseWidgets(
                self.discourse_actor, collection_refs
            )
            self.protocol_discourse_functions = [
                self.discourse_widgets.topics,
                self.discourse_widgets.users,
                self.discourse_widgets.categories,
                self.discourse_widgets.tags,
                self.discourse_widgets.top_topics,
                self.discourse_widgets.latest_topics,
                self.discourse_widgets.latest_posts,
                self.discourse_widgets.top_users,
            ]

        if protocol.get("developers", None):
            logger.info(
                f"Found dev reports configuration for protocol {self.protocol_name}."
            )
            self.developers_actor = DevelopersActor()
            self.developers_widget = DevelopersWidget(
                self.developers_actor, collection_refs, protocol["developers"]
            )
            self.protocol_developers_functions = [
                self.developers_widget.full_time,
                self.developers_widget.monthly_active_devs,
                self.developers_widget.total_repos,
                self.developers_widget.total_commits,
                self.developers_widget.monthly_active_dev_chart,
                self.developers_widget.total_monthly_active_dev_chart,
                self.developers_widget.dev_type_table,
                self.developers_widget.monthly_commits_by_dev_type_chart,
                self.developers_widget.monthly_commits_chart,
            ]

        if protocol.get("messari", None):
            logger.info(
                f"Found messari configuration for protocol {self.protocol_name}."
            )

            asset_key = protocol["messari"].get("asset_key", None)

            if asset_key is None:
                raise ValueError(
                    f"Messari configuration for protocol {self.protocol_name} is missing asset."
                )

            self.messari_actor = MessariActor()
            self.messari_widgets = MessariWidgets(
                self.messari_actor, collection_refs, asset_key
            )
            self.protocol_messari_functions = [
                self.messari_widgets.asset,
                self.messari_widgets.asset_profile,
                self.messari_widgets.asset_metrics,
                # self.messari_widgets.asset_timeseries,
            ]

        if protocol.get("governance", None):
            logger.info(
                f"Found governance configuration for protocol {self.protocol_name}."
            )
            governance_id = protocol["governance"].get("governance_id", None)
            organization_id = protocol["governance"].get(
                "organization_id", None
            )
            chain_id = protocol["governance"].get("chain_id", None)
            slug = protocol["governance"].get("slug", None)
            if (
                governance_id is None
                or organization_id is None
                or chain_id is None
                or slug is None
            ):
                raise ValueError(
                    f"Governance configuration for protocol {self.protocol_name} is missing governance_id, organization_id, chain_id or slug."
                )

            self.governance_actor = GovernanceActor()
            self.governance_widgets = GovernanceWidgets(
                self.governance_actor,
                collection_refs,
                governance_id,
                organization_id,
                chain_id,
                slug,
            )

            self.protocol_governance_functions = [
                self.governance_widgets.voting_power_chart,
                self.governance_widgets.delegates,
                self.governance_widgets.proposals,
                self.governance_widgets.governance_info,
                self.governance_widgets.safes,
            ]

        self.collection_refs = collection_refs
        repositories_ref_stream = self.db.collection(
            f"{self.protocol_name}-projects"
        ).stream()

        self.repositories = []

        # Print the data for each document
        for repository in repositories_ref_stream:
            self.repositories.append(repository.to_dict())

        logger.info(
            f"Found {len(self.repositories)} repositories for protocol {self.protocol_name} to be updated."
        )

        self.github_widgets = GithubWidgets(self.github_actor, collection_refs)
        self.github_cumulative = GithubCumulative(
            self.github_actor, collection_refs
        )

        self.project_pipeline_functions = [
            self.github_widgets.repository_info,
            self.github_widgets.commit_activity,
            self.github_widgets.code_frequency,
            self.github_widgets.participation,
            self.github_widgets.code_frequency,
            self.github_widgets.community_profile,
            self.github_widgets.punch_card,
            self.github_widgets.issue_count,
            (
                self.github_widgets.recent_issues,
                {"order_by": self.github_widgets.RecentIssuesOrder.CREATED_AT},
            ),
            (
                self.github_widgets.recent_issues,
                {"order_by": self.github_widgets.RecentIssuesOrder.UPDATED_AT},
            ),
            self.github_widgets.most_active_issues,
            self.github_widgets.pull_request_count,
            (
                self.github_widgets.recent_pull_requests,
                {
                    "order_by": self.github_widgets.RecentPullRequestsOrder.CREATED_AT
                },
            ),
            (
                self.github_widgets.recent_pull_requests,
                {
                    "order_by": self.github_widgets.RecentPullRequestsOrder.UPDATED_AT
                },
            ),
            self.github_widgets.language_breakdown,
            self.github_widgets.recent_stargazing_activity,
            self.github_widgets.recent_commits,
            self.github_widgets.recent_releases,
            self.github_widgets.contributors,  # paginated
            self.github_widgets.issue_activity,  # paginated
            self.github_widgets.pull_request_activity,  # paginated
            self.github_widgets.health_score,
        ]

        self.protocol_github_functions = [
            self.github_cumulative.cumulative_stats,
            self.github_cumulative.cumulative_commit_activity,
            self.github_cumulative.cumulative_code_frequency,
            self.github_cumulative.cumulative_participation,
            self.github_cumulative.cumulative_punch_card,
            self.github_cumulative.cumulative_issue_count,
            (
                self.github_cumulative.cumulative_recent_issues,
                {
                    "order_by": self.github_cumulative.CumulativeRecentIssuesOrder.CREATED_AT
                },
            ),
            (
                self.github_cumulative.cumulative_recent_issues,
                {
                    "order_by": self.github_cumulative.CumulativeRecentIssuesOrder.UPDATED_AT
                },
            ),
            self.github_cumulative.cumulative_most_active_issues,
            self.github_cumulative.cumulative_pull_request_count,
            (
                self.github_cumulative.cumulative_recent_pull_requests,
                {
                    "order_by": self.github_cumulative.CumulativeRecentPullRequestsOrder.CREATED_AT
                },
            ),
            (
                self.github_cumulative.cumulative_recent_pull_requests,
                {
                    "order_by": self.github_cumulative.CumulativeRecentPullRequestsOrder.UPDATED_AT
                },
            ),
            self.github_cumulative.cumulative_language_breakdown,
            self.github_cumulative.cumulative_recent_commits,
            self.github_cumulative.cumulative_recent_releases,
            self.github_cumulative.normalize_health_score,
        ]

    def run_pipelines(self):
        # self.run_project_github_pipeline()
        # self.run_protocol_pipeline(self.protocol_github_functions)
        # self.run_protocol_pipeline(self.protocol_discourse_functions)
        # self.run_protocol_pipeline(self.protocol_developers_functions)
        # self.run_protocol_pipeline(self.protocol_governance_functions)
        self.run_protocol_pipeline(self.protocol_messari_functions)

    def run_protocol_pipeline(self, protocol_pipeline):
        for item in protocol_pipeline:
            if isinstance(item, tuple) or isinstance(item, list):
                logging.info(
                    f"[*] Running cumulative github function: {item[0].__name__}"
                )
                if len(item) == 1:
                    func = item[0]
                    func()
                else:
                    func, args = item
                    func(**args)
            else:
                logging.info(
                    f"[*] Running cumulative github function: {item.__name__}"
                )
                item()

    def run_project_github_pipeline(self):
        # for repository in self.repositories:
        for repository in [
            {"owner": "lensterxyz", "repo": "lenster"},
            {"owner": "justmert", "repo": "eco-flow-frontend"},
            {"owner": "paritytech", "repo": "substrate"},
            {"owner": "thirdweb-example", "repo": "lens"},
        ]:
            is_valid = self.github_actor.check_repo_validity(
                repository["owner"], repository["repo"]
            )
            if not is_valid:
                logger.info(
                    f'Repository {repository["owner"]}/{repository["repo"]} is not valid.'
                )
                continue

            docs = (
                self.collection_refs["widgets"]
                .document("repositories")
                .collection(f"{repository['owner']}#{repository['repo']}")
                .get()
            )

            # Check if the collection is empty
            if not docs:
                logger.info(
                    f'Repository {repository["owner"]}/{repository["repo"]} does not exist in database. Creating it now.'
                )
                self.collection_refs["widgets"].document(
                    "repositories"
                ).collection(
                    f"{repository['owner']}#{repository['repo']}"
                ).document(
                    "dummy"
                ).set(
                    {}
                )

            logger.info(
                f'Running pipeline for repository {repository["owner"]}/{repository["repo"]}'
            )

            for item in self.project_pipeline_functions:
                if isinstance(item, tuple) or isinstance(item, list):
                    logging.info(
                        f"[*] Running pipeline function: {item[0].__name__}"
                    )
                    if len(item) == 1:
                        func = item[0]
                        func(repository["owner"], repository["repo"])
                    else:
                        func, args = item
                        func(repository["owner"], repository["repo"], **args)
                else:
                    logging.info(
                        f"[*] Running pipeline function: {item.__name__}"
                    )
                    item(repository["owner"], repository["repo"])

            logger.info(
                f'Finished running pipeline for repository {repository["owner"]}/{repository["repo"]}'
            )
