from fastapi import APIRouter
from fastapi import HTTPException

from fastapi import Path, HTTPException

from google.cloud import exceptions
from fastapi import HTTPException, Depends

from ..api import db, get_current_user

router = APIRouter()


@router.get(
    "/{protocol_name}/full-time",
    tags=["Developers - Ecosystem"],
    dependencies=[Depends(get_current_user)],
    responses={
        200: {
            "description": "Full Time",
            "content": {
                "application/json": {
                    "example": {
                        "count": 56,
                        "subtitle": "AS OF JUN-01-2023",
                        "title": "FULL-TIME DEVS",
                    }
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
def full_time(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the number of developers working full time on the protocol.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-developers")
            .document("full_time")
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
    "/{protocol_name}/monthly-active-devs",
    tags=["Developers - Ecosystem"],
    dependencies=[Depends(get_current_user)],
    responses={
        200: {
            "description": "Monthly Active Devs",
            "content": {
                "application/json": {
                    "example": {
                        "count": 203,
                        "subtitle": "AS OF JUN-01-2023",
                        "title": "MONTHLY ACTIVE DEVS",
                    }
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
def developers_monthly_active_devs(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the number of monthly active developers on the protocol.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-developers")
            .document("monthly_active_devs")
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
    "/{protocol_name}/total-repos",
    tags=["Developers - Ecosystem"],
    dependencies=[Depends(get_current_user)],
    responses={
        200: {
            "description": "Total Repos",
            "content": {
                "application/json": {
                    "example": {
                        "count": 2627,
                        "subtitle": "AS OF JUN-01-2023",
                        "title": "TOTAL FLOW REPOS",
                    }
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
def developers_total_repos(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the total number of repos on the protocol.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-developers")
            .document("total_repos")
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
    "/{protocol_name}/total-commits",
    tags=["Developers - Ecosystem"],
    dependencies=[Depends(get_current_user)],
    responses={
        200: {
            "description": "Total Repos",
            "content": {
                "application/json": {
                    "example": {
                        "count": 937888,
                        "subtitle": "AS OF JUN-01-2023",
                        "title": "TOTAL FLOW COMMITS",
                    }
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
def developers_total_commits(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the total number of commits on the protocol.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-developers")
            .document("total_commits")
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
    "/{protocol_name}/monthly-active-dev-chart",
    tags=["Developers - Ecosystem"],
    dependencies=[Depends(get_current_user)],
    responses={
        200: {
            "description": "Monthly Active Developers Chart",
            "content": {
                "application/json": {
                    "example": {
                        "yAxis": {},
                        "xAxis": {"type": "datetime"},
                        "series": [
                            {
                                "name": "Full-Time Developers",
                                "data": [{"date": "1423440000000", "value": 32}],
                            },
                            {
                                "name": "Part-Time Developers",
                                "data": [{"date": "1423440000000", "value": 15}],
                            },
                            {
                                "name": "One-Time Developers",
                                "data": [{"date": "1423440000000", "value": 23}],
                            },
                        ],
                    }
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
def developers_monthly_active_dev_chart(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the monthly active developers on the protocol in a chart format.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-developers")
            .document("monthly_active_dev_chart")
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
    "/{protocol_name}/total-monthly-active-dev-chart",
    tags=["Developers - Ecosystem"],
    dependencies=[Depends(get_current_user)],
    responses={
        200: {
            "description": "Total monthly Active Developers Chart",
            "content": {
                "application/json": {
                    "example": {
                        "yAxis": {},
                        "xAxis": {"type": "datetime"},
                        "series": [
                            {
                                "name": "Total Monthly Active Developers",
                                "data": [{"date": "1423440000000", "value": 32}],
                            }
                        ],
                    }
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
def developers_total_monthly_active_dev_chart(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the total monthly number of commits on the protocol in a chart format.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-developers")
            .document("total_monthly_active_dev_chart")
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
    "/{protocol_name}/dev-type-table",
    tags=["Developers - Ecosystem"],
    dependencies=[Depends(get_current_user)],
    responses={
        200: {
            "description": "Developer Type Table",
            "content": {
                "application/json": {
                    "example": {
                        "header": [
                            {
                                "index": "developer_type",
                                "title": "Developer Type",
                            },
                            {"index": "jun-01_2023", "title": "Jun-01 2023"},
                            {"index": "1y_%", "title": "1y %"},
                            {"index": "2y_%", "title": "2y %"},
                            {"index": "3y_%", "title": "3y %"},
                        ],
                        "rows": [
                            {
                                "1y_%": -44,
                                "2y_%": 0,
                                "3y_%": 250,
                                "developer_type": [
                                    "Total",
                                    "Only original code authors count toward developer numbers. Developers who merge pull requests, developers from forked commits, and bots are not counted as active developers.",
                                ],
                                "jun-01_2023": 203,
                                "key": "total",
                            },
                        ],
                    }
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
def developers_dev_type_table(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the counts based on developer types with changes in years. Returned data is in table format.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-developers")
            .document("dev_type_table")
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
    "/{protocol_name}/monthly-commits-by-dev-type-chart",
    tags=["Developers - Ecosystem"],
    dependencies=[Depends(get_current_user)],
    responses={
        200: {
            "description": "Monthly Commits by Developer Type Chart",
            "content": {
                "application/json": {
                    "example": {
                        "yAxis": {},
                        "xAxis": {"type": "datetime"},
                        "series": [
                            {
                                "name": "Full-Time Developers",
                                "data": [{"date": "1423440000000", "value": 32}],
                            },
                            {
                                "name": "Part-Time Developers",
                                "data": [{"date": "1423440000000", "value": 15}],
                            },
                            {
                                "name": "One-Time Developers",
                                "data": [{"date": "1423440000000", "value": 23}],
                            },
                        ],
                    }
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
def developers_monthly_commits_by_dev_type_chart(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the monthly commits by developer type in a chart format.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-developers")
            .document("monthly_commits_by_dev_type_chart")
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
    "/{protocol_name}/monthly-commits-chart",
    tags=["Developers - Ecosystem"],
    dependencies=[Depends(get_current_user)],
    responses={
        200: {
            "description": "Monthly Commits Chart",
            "content": {
                "application/json": {
                    "example": {
                        "yAxis": {},
                        "xAxis": {"type": "datetime"},
                        "series": [
                            {
                                "name": "Total",
                                "data": [{"date": "1423440000000", "value": 32}],
                            }
                        ],
                    }
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
def developers_monthly_commits_chart(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the total monthly commits in a chart format.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-developers")
            .document("monthly_commits_chart")
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
