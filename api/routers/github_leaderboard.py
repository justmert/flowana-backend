from fastapi import APIRouter
from fastapi import Depends, HTTPException
from fastapi import HTTPException
from fastapi import Path, HTTPException
from google.cloud import exceptions
from fastapi import Depends, HTTPException
from ..api import get_current_user, db

router = APIRouter()


@router.get(
    "/{protocol_name}/projects",
    dependencies=[Depends(get_current_user)],
    tags=["Github - Leaderboard"],
    responses={
        200: {
            "description": "Project leaderboard",
            "content": {
                "application/json": {
                    "example": [
                        {
                            "author": {
                                "login": "bigint",
                                "avatar_url": "https://avatars.githubusercontent.com/u/69431456?v=4",
                                "html_url": "https://github.com/bigint",
                            },
                            "contributions": {
                                "lensterxyz#lenster": {
                                    "owner": "lensterxyz",
                                    "repo": "lenster",
                                    "html_url": "https://github.com/lensterxyz/lenster",
                                    "commits": 4801,
                                }
                            },
                            "total_commits": 4801,
                        }
                    ]
                }
            },
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
def project_leaderboard(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns project leaderboard.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-leaderboard")
            .document(f"project_leaderboard")
            .get(field_paths=["data"])
            .to_dict()
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


@router.get(
    "/{protocol_name}/contributors",
    dependencies=[Depends(get_current_user)],
    tags=["Github - Leaderboard"],
    responses={
        200: {
            "description": "Project leaderboard",
            "content": {
                "application/json": {
                    "example": [
                        {
                            "contributions": {
                                "lensterxyz#lenster": {
                                    "owner": "lensterxyz",
                                    "repo": "lenster",
                                    "html_url": "https://github.com/lensterxyz/lenster",
                                    "commits": 4801,
                                }
                            },
                            "author": {
                                "avatar_url": "https://avatars.githubusercontent.com/u/69431456?v=4",
                                "html_url": "https://github.com/bigint",
                                "login": "bigint",
                            },
                            "total_commits": 4801,
                        }
                    ]
                }
            },
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
def project_contributors(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns contributor leaderboard.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-leaderboard")
            .document(f"contributor_leaderboard")
            .get(field_paths=["data"])
            .to_dict()
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
