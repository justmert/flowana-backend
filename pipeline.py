import logging
import tools.log_config as log_config
import json
from datetime import datetime
from enum import Enum
import pandas as pd
from datetime import timedelta
from github.github_actor import GithubActor
from github.github_widgets import GithubWidgets
from github.github_cumulative import GithubCumulative
from discourse.discourse_widgets import DiscourseWidgets
from discourse.discourse_actor import DiscourseActor
from developers.developers_widgets import DevelopersWidget
from developers.developers_actor import DevelopersActor
from governance.governance_actor import GovernanceActor
from governance.governance_widgets import GovernanceWidgets
from messari.messari_actor import MessariActor
from messari.messari_widgets import MessariWidgets

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, app, db, github_actor):
        self.app = app
        self.db = db
        self.repositories = []
        self.project_pipeline_functions = []
        self.collection_refs = {}
        self.github_actor = github_actor

    def build_discourse_pipeline(self, protocol):
        if protocol.get("forum", None):
            logger.info(f"Found forum configuration for protocol {self.protocol_name}.")
            forum_base_url = protocol["forum"][:-1] if protocol["forum"].endswith("/") else protocol["forum"]
            self.discourse_actor = DiscourseActor(forum_base_url)
            self.discourse_widgets = DiscourseWidgets(self.discourse_actor, self.collection_refs)
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

    def build_governance_pipeline(self, protocol):
        if protocol.get("governance", None):
            logger.info(f"Found governance configuration for protocol {self.protocol_name}.")
            governance_id = protocol["governance"].get("governance_id", None)
            organization_id = protocol["governance"].get("organization_id", None)
            chain_id = protocol["governance"].get("chain_id", None)
            slug = protocol["governance"].get("slug", None)
            if governance_id is None or organization_id is None or chain_id is None or slug is None:
                raise ValueError(
                    f"Governance configuration for protocol {self.protocol_name} is missing governance_id, organization_id, chain_id or slug."
                )

            self.governance_actor = GovernanceActor()
            self.governance_widgets = GovernanceWidgets(
                self.governance_actor,
                self.collection_refs,
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

    def build_messari_pipeline(self, protocol):
        if protocol.get("messari", None):
            logger.info(f"Found messari configuration for protocol {self.protocol_name}.")

            asset_key = protocol["messari"].get("asset_key", None)

            if asset_key is None:
                raise ValueError(f"Messari configuration for protocol {self.protocol_name} is missing asset.")

            self.messari_actor = MessariActor()
            self.messari_widgets = MessariWidgets(self.messari_actor, self.collection_refs, asset_key)
            self.protocol_messari_functions = [
                self.messari_widgets.asset,
                self.messari_widgets.asset_profile,
                self.messari_widgets.asset_metrics,
                self.messari_widgets.asset_timeseries,
            ]

    def build_developer_pipeline(self, protocol):
        if protocol.get("developers", None):
            logger.info(f"Found dev reports configuration for protocol {self.protocol_name}.")
            self.developers_actor = DevelopersActor()
            self.developers_widget = DevelopersWidget(
                self.developers_actor, self.collection_refs, protocol["developers"]
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

    def build_github_pipeline(self):
        self.github_widgets = GithubWidgets(self.github_actor, self.collection_refs)
        self.github_cumulative = GithubCumulative(self.github_actor, self.collection_refs)

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
                {"order_by": self.github_widgets.RecentPullRequestsOrder.CREATED_AT},
            ),
            (
                self.github_widgets.recent_pull_requests,
                {"order_by": self.github_widgets.RecentPullRequestsOrder.UPDATED_AT},
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
                {"order_by": self.github_cumulative.CumulativeRecentIssuesOrder.CREATED_AT},
            ),
            (
                self.github_cumulative.cumulative_recent_issues,
                {"order_by": self.github_cumulative.CumulativeRecentIssuesOrder.UPDATED_AT},
            ),
            self.github_cumulative.cumulative_most_active_issues,
            self.github_cumulative.cumulative_pull_request_count,
            (
                self.github_cumulative.cumulative_recent_pull_requests,
                {"order_by": self.github_cumulative.CumulativeRecentPullRequestsOrder.CREATED_AT},
            ),
            (
                self.github_cumulative.cumulative_recent_pull_requests,
                {"order_by": self.github_cumulative.CumulativeRecentPullRequestsOrder.UPDATED_AT},
            ),
            self.github_cumulative.cumulative_language_breakdown,
            self.github_cumulative.cumulative_recent_commits,
            self.github_cumulative.cumulative_recent_releases,
            self.github_cumulative.normalize_health_score,
        ]

    def contruct_pipeline(self, protocol, collection_refs):
        self.protocol_name = protocol["name"]
        self.collection_refs = collection_refs

        self.build_github_pipeline()

        self.build_discourse_pipeline(protocol)

        self.build_developer_pipeline(protocol)

        self.build_messari_pipeline(protocol)

        self.build_governance_pipeline(protocol)

        repositories_ref_stream = self.db.collection(f"{self.protocol_name}-projects").stream()

        # Print the data for each document
        for repository in repositories_ref_stream:
            print(repository.id)
            self.repositories.append(repository.to_dict())

        logger.info(
            f"[*] {len(self.repositories)} indexed repositories for protocol {self.protocol_name} will be updated."
        )

    def run_pipelines(self):
        # self.run_project_pipeline("Github Project", self.project_pipeline_functions)
        # self.run_protocol_pipeline("Github Cumulative", self.protocol_github_functions)
        self.run_protocol_pipeline("Discourse", self.protocol_discourse_functions)
        self.run_protocol_pipeline("Developers", self.protocol_developers_functions)
        self.run_protocol_pipeline("Governance", self.protocol_governance_functions)
        self.run_protocol_pipeline("Messari", self.protocol_messari_functions)
        pass

    def function_executer(self, f, *args, **kwargs):
        logging.info(f"[...] Running function: {f.__name__}")
        try:
            if len(args) and len(kwargs):
                f(*args, **kwargs)

            elif len(args):
                f(*args)

            elif len(kwargs):
                f(**kwargs)

            else:
                f()

        except Exception as e:
            logging.info(f"[#ERR] Error running function: {f.__name__} error: {e}")

        else:
            logging.info(f"[*] Completed running function: {f.__name__}")

    def run_protocol_pipeline(self, pipeline_name, protocol_pipeline):
        for i, item in enumerate(protocol_pipeline):
            logger.info(f"[===] {self.protocol_name.upper()}/{pipeline_name} - [{i + 1}/{len(protocol_pipeline)}]")
            if isinstance(item, tuple) or isinstance(item, list):
                if len(item) == 1:
                    self.function_executer(item[0])
                else:
                    self.function_executer(item[0], **item[1])
            else:
                self.function_executer(item)

    def run_project_pipeline(self, pipeline_name, project_pipeline):
        for repository in self.repositories:
            if not repository:
                continue

            is_valid = self.github_actor.check_repo_validity(repository["owner"], repository["repo"])
            if not is_valid:
                logger.warning(f'[!] Repository {repository["owner"]}/{repository["repo"]} is not valid.')
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
                self.collection_refs["widgets"].document("repositories").collection(
                    f"{repository['owner']}#{repository['repo']}"
                ).document("dummy").set({})

            logger.info(f'Running pipeline for repository {repository["owner"]}/{repository["repo"]}')

            for i, item in enumerate(project_pipeline):
                logger.info(f"[===] {self.protocol_name.upper()}/{pipeline_name} - [{i + 1}/{len(project_pipeline)}]")
                if isinstance(item, tuple) or isinstance(item, list):
                    if len(item) == 1:
                        self.function_executer(func, repository["owner"], repository["repo"])
                    else:
                        func, args = item
                        self.function_executer(func, repository["owner"], repository["repo"], **args)
                else:
                    self.function_executer(item, repository["owner"], repository["repo"])

            logger.info(f'Finished running pipeline for repository {repository["owner"]}/{repository["repo"]}')
