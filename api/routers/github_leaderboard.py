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
                            "repository_info": {
                                "created_at": "2022-03-19T15:01:46Z",
                                "description": "Lenster is a decentralized and permissionless social media app built with Lens Protocol 🌿",
                                "release_count": 17,
                                "owner_login": "lensterxyz",
                                "is_empty": False,
                                "stargazer_count": 20884,
                                "categories.lvl0": [
                                    "web3",
                                    "blockchain",
                                    "graphql",
                                    "hacktoberfest",
                                    "nextjs",
                                    "react",
                                    "social-media",
                                    "typescript",
                                    "arweave",
                                    "ipfs",
                                    "lens-protocol",
                                    "polygon",
                                    "turborepo",
                                    "tailwindcss",
                                    "wagmi",
                                    "dapp",
                                    "playwright",
                                ],
                                "watcher_count": 101,
                                "url": "https://github.com/lensterxyz/lenster",
                                "updated_at": "2023-08-07T09:06:06Z",
                                "owner_avatar_url": "https://avatars.githubusercontent.com/u/103585522?v=4",
                                "default_branch_commit_count": 6644,
                                "disk_usage": 31762,
                                "commit_comment_count": 5053,
                                "pull_request_count": 2245,
                                "environment_count": 8,
                                "issue_count": 1190,
                                "primary_language_color": "#3178c6",
                                "valid": True,
                                "is_archived": False,
                                "is_closed": False,
                                "fork_count": 1262,
                                "is_fork": False,
                                "primary_language_name": "TypeScript",
                            },
                            "health_score": {
                                "total": 50,
                                "pull_request_activity": 50,
                                "commit_activity": 50,
                                "contribution_activity": 50,
                                "issue_activity": 50,
                                "grade": "C+",
                                "release_activity": 50,
                            },
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
            "description": "Contributor Leaderboard",
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
