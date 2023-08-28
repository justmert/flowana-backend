from datetime import datetime
from enum import Enum
from datetime import timezone


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
    rfc_format = datetime.now(timezone.utc).isoformat() + "Z"
    collection_ref.document(pipeline_name).set({"data": rfc_format})


def format_time_duration(seconds):
    """Format time duration in days, hours, and minutes."""
    days, remainder = divmod(seconds, 86400)  # 86400 seconds in a day
    hours, remainder = divmod(remainder, 3600)  # 3600 seconds in an hour
    minutes, _ = divmod(remainder, 60)  # 60 seconds in a minute

    duration = []
    if days:
        duration.append(f"{int(days)}d")
    if hours:
        duration.append(f"{int(hours)}h")
    if minutes:
        duration.append(f"{int(minutes)}m")

    return " ".join(duration)
