from fastapi import APIRouter
from fastapi import Depends, HTTPException
from enum import Enum
from fastapi import HTTPException
from fastapi import Query, Path, HTTPException
from google.cloud import exceptions
from fastapi import Depends
from ..api import get_current_user, db
from tools.helpers import PipelineType

router = APIRouter()


@router.get(
    "/{protocol_name}/last-updated",
    tags=["Misc"],
    dependencies=[Depends(get_current_user)],
    responses={
        200: {
            "description": "Last updated timestamp",
            "content": {"application/json": {"example": None}},
        },
        204: {
            "description": "No content found.",
            "content": {"application/json": {"example": None}},
        },
        404: {
            "description": "Not found",
            "content": {"application/json": {"example": {"error": "Error description"}}},
        },
    },
)
def get_last_updated(
    protocol_name: str = Path(..., description="Protocol name"),
    page_type: PipelineType = Query(PipelineType.GITHUB_PROJECTS, description="Page type"),
):
    """
    Returns the last updated timestamp for the protocol.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-last-updated").document(page_type.value).get(field_paths=["data"]).to_dict()
        )

        if ref is None:
            raise exceptions.NotFound("Collection or document not found")

    except exceptions.NotFound as ex:
        # Handle case where document or collection does not exist
        raise HTTPException(status_code=404, detail=str(ex))

    except Exception as e:
        # Handle other exceptions
        raise HTTPException(status_code=500, detail=f"An error occurred {str(e)}")

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data
