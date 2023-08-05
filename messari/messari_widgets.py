from .messari_actor import MessariActor
import logging
import tools.log_config as log_config
import datetime

logger = logging.getLogger(__name__)


class MessariWidgets:
    def __init__(self, actor: MessariActor, collection_refs, asset_key):
        self.actor = actor
        self.collection_refs = collection_refs
        self.asset_key = asset_key

    def is_valid(self, response):
        if response is None:
            return False

        elif isinstance(response, dict) and not response:
            return False

        elif isinstance(response, list) and not response:
            return False

        return True

    def asset(self, **kwargs):
        data = self.actor.messari_rest_make_request(url=f"/v1/assets/{self.asset_key}", max_page_fetch=1)

        if not self.is_valid(data):
            logger.warning("[!] Invalid or empty data returned")
            return

        self.collection_refs["messari"].document("asset").set({"data": data})

    def asset_profile(self, **kwargs):
        data = self.actor.messari_rest_make_request(url=f"/v2/assets/{self.asset_key}/profile", max_page_fetch=1)

        if not self.is_valid(data):
            logger.warning("[!] Invalid or empty data returned")
            return

        self.collection_refs["messari"].document("asset_profile").set({"data": data})

    def asset_metrics(self, **kwargs):
        data = self.actor.messari_rest_make_request(url=f"/v1/assets/{self.asset_key}/metrics", max_page_fetch=1)

        if not self.is_valid(data):
            logger.warning("[!] Invalid or empty data returned")
            return

        self.collection_refs["messari"].document("asset_metrics").set({"data": data})

    def asset_metrics(self, **kwargs):
        data = self.actor.messari_rest_make_request(url=f"/v1/assets/{self.asset_key}/metrics", max_page_fetch=1)

        if not self.is_valid(data):
            logger.warning("[!] Invalid or empty data returned")
            return

        self.collection_refs["messari"].document("asset_metrics").set({"data": data})

    def asset_timeseries(self, **kwargs):
        data = self.actor.messari_rest_make_request(url=f"/v1/assets/metrics", max_page_fetch=1)

        if not self.is_valid(data):
            logger.warning("[!] Invalid or empty data returned")
            return

        for metric in data["data"]["metrics"]:
            if not metric.get("role_restriction", None):
                for interval in ["1d", "1w"]:
                    metric_id = metric["metric_id"]
                    metric_name = metric["name"]

                    logger.info(f"[asset_timeseries] [{metric_id}-{interval}] {metric_name}")
                    query_params = {
                        "interval": interval,
                        "timestamp-format": "rfc3339",
                    }
                    data = self.actor.messari_rest_make_request(
                        url=f"/v1/assets/{self.asset_key}/metrics/{metric_id}/time-series",
                        max_page_fetch=1,
                        variables=query_params,
                    )

                    if not self.is_valid(data) or not data["data"].get("values", None):
                        logger.warning("[!] Invalid or empty data returned")
                        continue

                    data["data"]["values"] = [
                        dict(zip(data["data"]["parameters"]["columns"], v)) for v in data["data"]["values"]
                    ]
                    self.collection_refs["messari"].document(f"asset_timeseries_{metric_id}_{interval}").set(
                        {"data": data}
                    )

        indexed_timeseries_list = []
        ref = self.collection_refs["messari"].stream()
        for doc in ref:
            if str(doc.id).startswith("asset_timeseries"):
                indexed_timeseries_list.append(doc.id)

        self.collection_refs["messari"].document("indexed_timeseries_list").set({"data": indexed_timeseries_list})
