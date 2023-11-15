from fastapi import APIRouter
from fastapi import Depends, HTTPException
from enum import Enum
from fastapi import HTTPException
from fastapi import Query, Path, HTTPException
from google.cloud import exceptions
from fastapi import Depends, HTTPException
from ..api import get_current_user, db
from .github_project import RecentIssuesOrder, RecentPullRequestOrder

router = APIRouter()


@router.get(
    "/{protocol_name}/stats",
    dependencies=[Depends(get_current_user)],
    tags=["Github - Ecosystem"],
    responses={
        200: {
            "description": "Cumulative Stats",
            "content": {
                "application/json": {
                    "example": [
                        {
                            "commit_comment_count": 6957,
                            "default_branch_commit_count": 9333,
                            "disk_usage": 52013,
                            "environment_count": 17,
                            "fork_count": 2174,
                            "issue_count": 1302,
                            "pull_request_count": 2974,
                            "release_count": 19,
                            "stargazers_count": 0,
                            "watcher_count": 178,
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
            "content": {
                "application/json": {"example": {"error": "Error description"}}
            },
        },
    },
)
def stats(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the cumulative stats for the protocol.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-cumulative")
            .document("cumulative_info")
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
    "/{protocol_name}/commit-activity",
    dependencies=[Depends(get_current_user)],
    tags=["Github - Ecosystem"],
    responses={
        200: {
            "description": "Cumulative Commit Activity",
            "content": {
                "application/json": {
                    "example": [
                        {
                            "days": [0, 3, 26, 20, 39, 1, 0],
                            "total": 89,
                            "week": 1336280400,
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
            "content": {
                "application/json": {"example": {"error": "Error description"}}
            },
        },
    },
)
def commit_activity(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the cumulative commit activity for the protocol.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-cumulative")
            .document("cumulative_commit_activity")
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
    "/{protocol_name}/participation",
    dependencies=[Depends(get_current_user)],
    tags=["Github - Ecosystem"],
    responses={
        200: {
            "description": "Cumulative Participation",
            "content": {
                "application/json": {
                    "example": [
                        {
                            "xAxis": {"type": "category"},
                            "yAxis": {"type": "value"},
                            "series": [
                                {
                                    "name": "All",
                                    "data": [3, 5, 7],
                                    "type": "line",
                                },
                                {
                                    "name": "Owners",
                                    "data": [1, 2, 3],
                                    "type": "line",
                                },
                                {
                                    "name": "Others",
                                    "data": [2, 3, 4],
                                    "type": "line",
                                },
                            ],
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
            "content": {
                "application/json": {"example": {"error": "Error description"}}
            },
        },
    },
)
def participation(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the cumulative participation for the protocol.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-cumulative")
            .document("cumulative_participation")
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
    "/{protocol_name}/code-frequency",
    dependencies=[Depends(get_current_user)],
    tags=["Github - Ecosystem"],
    responses={
        200: {
            "description": "Cumulative Code Frequency",
            "content": {
                "application/json": {
                    "example": [
                        {
                            "xAxis": {"type": "category"},
                            "yAxis": {"type": "value"},
                            "series": [
                                {
                                    "name": "All",
                                    "data": [3, 5, 7],
                                    "type": "line",
                                },
                                {
                                    "name": "Owners",
                                    "data": [1, 2, 3],
                                    "type": "line",
                                },
                                {
                                    "name": "Others",
                                    "data": [2, 3, 4],
                                    "type": "line",
                                },
                            ],
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
            "content": {
                "application/json": {"example": {"error": "Error description"}}
            },
        },
    },
)
def code_frequency(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the cumulative code frequency for the protocol.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-cumulative")
            .document("cumulative_code_frequency")
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
    "/{protocol_name}/punch-card",
    dependencies=[Depends(get_current_user)],
    tags=["Github - Ecosystem"],
    responses={
        200: {
            "description": "Cumulative Punch Card",
            "content": {
                "application/json": {
                    "example": [
                        {"day": 0, "hour": 0, "commits": 4},
                        {"day": 0, "hour": 1, "commits": 24},
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
            "content": {
                "application/json": {"example": {"error": "Error description"}}
            },
        },
    },
)
def punch_card(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the cumulative punch card for the protocol.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-cumulative")
            .document("cumulative_punch_card")
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
    "/{protocol_name}/language-breakdown",
    dependencies=[Depends(get_current_user)],
    tags=["Github - Ecosystem"],
    responses={
        200: {
            "description": "Cumulative Language Breakdown",
            "content": {
                "application/json": {
                    "example": [
                        {"name": "JavaScript", "percentage": 100, "size": 55852}
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
            "content": {
                "application/json": {"example": {"error": "Error description"}}
            },
        },
    },
)
def language_breakdown(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the cumulative language breakdown for the protocol.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-cumulative")
            .document("cumulative_language_breakdown")
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
    "/{protocol_name}/issue-count",
    dependencies=[Depends(get_current_user)],
    tags=["Github - Ecosystem"],
    responses={
        200: {
            "description": "Cumulative Issue Count",
            "content": {"application/json": {"example": {"closed": 12, "open": 2}}},
        },
        204: {
            "description": "No content found.",
            "content": {"application/json": {"example": None}},
        },
        404: {
            "description": "Not found",
            "content": {
                "application/json": {"example": {"error": "Error description"}}
            },
        },
    },
)
def issue_count(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the cumulative issue count for the protocol.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-cumulative")
            .document("cumulative_issue_count")
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


class CumulativeMostActiveIssuesInterval(str, Enum):
    day = "day"
    week = "week"
    month = "month"
    year = "year"


@router.get(
    "/{protocol_name}/most-active-issues",
    dependencies=[Depends(get_current_user)],
    tags=["Github - Ecosystem"],
    responses={
        200: {
            "description": "Cumulative Most Active Issues",
            "content": {
                "application/json": {
                    "example": [
                        {
                            "author_avatar_url": "https://avatars.githubusercontent.com/u/10109867?u=01b740a684334ebda118d856d6986ca36b6a05c9&v=4",
                            "author_login": "jounih",
                            "closed": False,
                            "closed_at": None,
                            "comments_count": 5,
                            "created_at": "2022-10-13T09:16:51Z",
                            "number": 808,
                            "state": "OPEN",
                            "title": "New composer design",
                            "updated_at": "2023-07-04T10:52:51Z",
                            "owner": "lensterxyz",
                            "repo": "lenster",
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
            "content": {
                "application/json": {"example": {"error": "Error description"}}
            },
        },
    },
)
def most_active_issues(
    protocol_name: str = Path(..., description="Protocol name"),
    interval: CumulativeMostActiveIssuesInterval = Query(
        ...,
        description="Interval for which the most active issues are to be returned",
    ),
):
    """
    Returns the cumulative most active issues for the protocol.

    `interval` can be one of the following: day, week, month, year

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-cumulative")
            .document("cumulative_most_active_issues")
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

    interval_data = data.get(interval, [])
    if not interval_data:
        raise HTTPException(status_code=204, detail="No data for the given interval.")

    return interval_data


@router.get(
    "/{protocol_name}/pull-request-count",
    dependencies=[Depends(get_current_user)],
    tags=["Github - Ecosystem"],
    responses={
        200: {
            "description": "Cumulative Pull Request Count",
            "content": {"application/json": {"example": {"closed": 12, "open": 2}}},
        },
        204: {
            "description": "No content found.",
            "content": {"application/json": {"example": None}},
        },
        404: {
            "description": "Not found",
            "content": {
                "application/json": {"example": {"error": "Error description"}}
            },
        },
    },
)
def pull_request_count(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the cumulative pull request count for the protocol.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-cumulative")
            .document("cumulative_pull_request_count")
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


class CumulativeRecentIssuesOrder(str, Enum):
    created_at = "created_at"
    updated_at = "updated_at"


@router.get(
    "/{protocol_name}/recent-issues",
    dependencies=[Depends(get_current_user)],
    tags=["Github - Ecosystem"],
    responses={
        200: {
            "description": "Cumulative Recent Issues",
            "content": {
                "application/json": {
                    "example": [
                        {
                            "author_avatar_url": "https://avatars.githubusercontent.com/u/137943459?v=4",
                            "author_login": "jimmy-houdini",
                            "comments_count": 0,
                            "created_at": "2023-07-03T20:37:49Z",
                            "number": 3213,
                            "state": "OPEN",
                            "title": "CONNECTOR NOT FOUND",
                            "updated_at": "2023-07-03T23:28:53Z",
                            "owner": "lensterxyz",
                            "repo": "lenster",
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
            "content": {
                "application/json": {"example": {"error": "Error description"}}
            },
        },
    },
)
def recent_issues(
    protocol_name: str = Path(..., description="Protocol name"),
    order_by: CumulativeRecentIssuesOrder = Query(..., description="Order by field"),
):
    """
    Returns the cumulative recent issues for the protocol.

    `order_by` can be one of the following values: created_at, updated_at

    """

    field_name = None
    if order_by == RecentIssuesOrder.created_at:
        field_name = "recent_created_issues"

    elif order_by == RecentIssuesOrder.updated_at:
        field_name = "recent_updated_issues"

    try:
        ref = (
            db.collection(f"{protocol_name}-cumulative")
            .document(f"cumulative_{field_name}")
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
    "/{protocol_name}/recent-pull-requests",
    dependencies=[Depends(get_current_user)],
    tags=["Github - Ecosystem"],
    responses={
        200: {
            "description": "Cumulative Recent Pull Requests",
            "content": {
                "application/json": {
                    "example": [
                        {
                            "author_avatar_url": "https://avatars.githubusercontent.com/u/69431456?u=8b8a4ccce26e41600aa5db78c2e4b148445f5efa&v=4",
                            "author_login": "bigint",
                            "comments_count": 1,
                            "created_at": "2023-07-03T18:03:24Z",
                            "number": 3212,
                            "state": "OPEN",
                            "title": "chore: update dependencies 📦",
                            "updated_at": "2023-07-04T05:28:28Z",
                            "owner": "lensterxyz",
                            "repo": "lenster",
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
            "content": {
                "application/json": {"example": {"error": "Error description"}}
            },
        },
    },
)
def recent_pull_requests(
    protocol_name: str = Query(..., description="Protocol name"),
    order_by: RecentPullRequestOrder = Query(..., description="Order by field"),
):
    """
    Returns the cumulative recent pull requests for the protocol.

    `order_by` can be one of the following values: created_at, updated_at

    """

    field_name = None
    if order_by == RecentIssuesOrder.created_at:
        field_name = "recent_created_pull_requests"

    elif order_by == RecentIssuesOrder.updated_at:
        field_name = "recent_updated_pull_requests"

    try:
        ref = (
            db.collection(f"{protocol_name}-cumulative")
            .document(f"cumulative_{field_name}")
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
    "/{protocol_name}/recent-commits",
    dependencies=[Depends(get_current_user)],
    tags=["Github - Ecosystem"],
    responses={
        200: {
            "description": "Cumulative Recent Commits",
            "content": {
                "application/json": {
                    "example": [
                        {
                            "author_avatar_url": "https://avatars.githubusercontent.com/u/69431456?v=4",
                            "author_login": "bigint",
                            "committed_date": "2023-07-04T06:58:53Z",
                            "message": "fix: add publicationsProfileBookmarks to field policy",
                            "url": "https://github.com/lensterxyz/lenster/commit/f0e21ed226b0ae7a792ee118443caf3bdb20f371",
                            "owner": "lensterxyz",
                            "repo": "lenster",
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
            "content": {
                "application/json": {"example": {"error": "Error description"}}
            },
        },
    },
)
def recent_commits(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the cumulative recent commits for the protocol.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-cumulative")
            .document("cumulative_recent_commits")
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
    "/{protocol_name}/recent-releases",
    dependencies=[Depends(get_current_user)],
    tags=["Github - Ecosystem"],
    responses={
        200: {
            "description": "Cumulative Recent Releases",
            "content": {
                "application/json": {
                    "example": [
                        {
                            "name": "v1.0.9-beta",
                            "published_at": "2023-05-02T04:52:21Z",
                            "tag_name": "v1.0.9-beta",
                            "url": "https://github.com/lensterxyz/lenster/releases/tag/v1.0.9-beta",
                            "owner": "lensterxyz",
                            "repo": "lenster",
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
            "content": {
                "application/json": {"example": {"error": "Error description"}}
            },
        },
    },
)
def recent_releases(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the cumulative recent releases for the protocol.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-cumulative")
            .document("cumulative_recent_releases")
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
