from discourse_actor import DiscourseActor
import logging
import log_config
import json
from datetime import datetime
from enum import Enum
import pandas as pd
from datetime import timedelta

logger = logging.getLogger(__name__)


class DiscourseWidgets():

    def __init__(self, actor: DiscourseActor, collection_refs):
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

    def categories(self, **kwargs):
        # formatting will be in frontend
        data = self.actor.discourse_rest_make_request(
            f'/categories')
        
        print(data)

        if not self.is_valid(data):
            logger.info(
                f'[#invalid] No categories for protocol ')
            return

        
