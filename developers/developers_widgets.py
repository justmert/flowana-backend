from .developers_actor import DevelopersActor
import logging
import tools.log_config as log_config
import json
from datetime import datetime
from enum import Enum
import pandas as pd
from datetime import timedelta

logger = logging.getLogger(__name__)


class DevelopersWidget:
    def __init__(self, actor: DevelopersActor, collection_refs, developer_ecosystem):
        self.actor = actor
        self.collection_refs = collection_refs
        self.developer_ecosystem = developer_ecosystem

    def is_valid(self, response):
        if response is None:
            return False

        elif isinstance(response, dict) and not response:
            return False

        elif isinstance(response, list) and not response:
            return False

        return True

    def full_time(self, **kwargs):
        data = self.actor.developer_rest_make_request(
            url=f"/api/stats/mau/{self.developer_ecosystem}",
            variables={"type": "full_time"},
        )

        if not self.is_valid(data):
            logger.warning("[!] Invalid or empty data returned")
            return

        formatted_data = {
            "title": data["subtitle"],
            "count": int(data["title"].replace(",", "")),
            "subtitle": data["footnote"],
        }
        self.collection_refs["developers"].document("full_time").set({"data": formatted_data})

    def monthly_active_devs(self, **kwargs):
        data = self.actor.developer_rest_make_request(url=f"/api/stats/mau/{self.developer_ecosystem}", variables={})

        if not self.is_valid(data):
            logger.warning("[!] Invalid or empty data returned")
            return

        formatted_data = {
            "title": data["subtitle"],
            "count": int(data["title"].replace(",", "")),
            "subtitle": data["footnote"],
        }
        self.collection_refs["developers"].document("monthly_active_devs").set({"data": formatted_data})

    def total_repos(self, **kwargs):
        data = self.actor.developer_rest_make_request(
            url=f"/api/stats/total_repos/{self.developer_ecosystem}",
            variables={},
        )

        if not self.is_valid(data):
            logger.warning("[!] Invalid or empty data returned")
            return

        formatted_data = {
            "title": data["subtitle"],
            "count": int(data["title"].replace(",", "")),
            "subtitle": data["footnote"],
        }
        self.collection_refs["developers"].document("total_repos").set({"data": formatted_data})

    def total_commits(self, **kwargs):
        data = self.actor.developer_rest_make_request(
            url=f"/api/stats/total_commits/{self.developer_ecosystem}",
            variables={},
        )

        if not self.is_valid(data):
            logger.warning("[!] Invalid or empty data returned")
            return

        formatted_data = {
            "title": data["subtitle"],
            "count": int(data["title"].replace(",", "")),
            "subtitle": data["footnote"],
        }
        self.collection_refs["developers"].document("total_commits").set({"data": formatted_data})

    def monthly_active_dev_chart(self, **kwargs):
        data = self.actor.developer_rest_make_request(
            url=f"/api/charts/dev_mau_by_dev_type/{self.developer_ecosystem}",
            variables={},
        )

        if not self.is_valid(data):
            logger.warning("[!] Invalid or empty data returned")
            return

        for series in data["series"]:
            series["data"] = [{"date": x[0], "value": x[1]} for x in series["data"]]

        formatted_data = {
            "xAxis": {"type": data["xAxis"]["type"]},
            "yAxis": {},
            "series": data["series"],
        }
        self.collection_refs["developers"].document("monthly_active_dev_chart").set({"data": formatted_data})

    def total_monthly_active_dev_chart(self, **kwargs):
        data = self.actor.developer_rest_make_request(
            url=f"/api/charts/dev_mau/{self.developer_ecosystem}", variables={}
        )

        if not self.is_valid(data):
            logger.warning("[!] Invalid or empty data returned")
            return

        for series in data["series"]:
            series["data"] = [{"date": x[0], "value": x[1]} for x in series["data"]]

        formatted_data = {
            "xAxis": {"type": data["xAxis"]["type"]},
            "yAxis": {},
            "series": data["series"],
        }
        self.collection_refs["developers"].document("total_monthly_active_dev_chart").set({"data": formatted_data})

    def dev_type_table(self, **kwargs):
        data = self.actor.developer_rest_make_request(
            url=f"/api/tables/devs_by_type_stats/{self.developer_ecosystem}",
            variables={},
        )

        if not self.is_valid(data):
            logger.warning("[!] Invalid or empty data returned")
            return

        # Extract headers
        header = [{"title": column["title"], "index": column["dataIndex"]} for column in data["columns"]]
        data = {
            "header": header,
            "rows": data["dataSource"],
        }
        self.collection_refs["developers"].document("dev_type_table").set({"data": data})

    def monthly_commits_by_dev_type_chart(self, **kwargs):
        data = self.actor.developer_rest_make_request(
            url=f"/api/charts/monthly_commits_by_dev_type/{self.developer_ecosystem}",
            variables={},
        )

        if not self.is_valid(data):
            logger.warning("[!] Invalid or empty data returned")
            return

        for series in data["series"]:
            series["data"] = [{"date": x[0], "value": x[1]} for x in series["data"]]

        formatted_data = {
            "xAxis": {"type": data["xAxis"]["type"]},
            "yAxis": {},
            "series": data["series"],
        }
        self.collection_refs["developers"].document("monthly_commits_by_dev_type_chart").set({"data": formatted_data})

    def monthly_commits_chart(self, **kwargs):
        data = self.actor.developer_rest_make_request(
            url=f"/api/charts/monthly_commits/{self.developer_ecosystem}",
            variables={},
        )

        if not self.is_valid(data):
            logger.warning("[!] Invalid or empty data returned")
            return

        for series in data["series"]:
            series["data"] = [{"date": x[0], "value": x[1]} for x in series["data"]]

        formatted_data = {
            "xAxis": {"type": data["xAxis"]["type"]},
            "yAxis": {},
            "series": data["series"],
        }
        self.collection_refs["developers"].document("monthly_commits_chart").set({"data": formatted_data})
