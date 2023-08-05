import datetime
from enum import Enum

def write_last_updated(collection_ref, **kwargs):
    # datetime in rfc3339 format
    rfc_format = datetime.now().isoformat() + "Z"
    collection_ref.document(f"last_updated_at").set({"data": rfc_format})

