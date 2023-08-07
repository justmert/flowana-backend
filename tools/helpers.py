from datetime import datetime
from enum import Enum


class PipelineType(str, Enum):
    GITHUB_CUMULATIVE = "cumulative"
    GITHUB_PROJECTS = "projects"
    GITHUB_LEADERBOARD = "leaderboard"
    DISCOURSE = "discourse"
    DEVELOPERS = "developers"
    GOVERNANCE = "governance"
    MESSARI = "messari"
    CRAWLER = "crawler"


def write_last_updated(collection_ref, pipeline_name, **kwargs):
    # datetime in rfc3339 format
    rfc_format = datetime.now().isoformat() + "Z"
    collection_ref.document(pipeline_name).set({"data": rfc_format})
