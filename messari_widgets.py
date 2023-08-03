from messari_actor import MessariActor
import logging
import log_config
import json
from datetime import datetime
from enum import Enum
import pandas as pd
from datetime import timedelta

logger = logging.getLogger(__name__)


class DiscourseWidgets():

    def __init__(self, actor: MessariActor, collection_refs):
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

    def messari_asset(self, **kwargs):

        self.collection_refs['discourse'].document('topic_metrics').set({'data': None})

