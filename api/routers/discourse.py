from fastapi import APIRouter
from pydantic import BaseModel
from datetime import datetime
import json
from typing import Union
from fastapi import FastAPI, Response
import pandas as pd
from fastapi import Depends, HTTPException, status
from typing import Any
import os
import firebase_admin
from firebase_admin import firestore, credentials
from dotenv import load_dotenv
from enum import Enum
import pandas as pd
from collections import defaultdict
from fastapi import HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from fastapi.openapi.models import Example
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from fastapi import FastAPI, Query, Path, HTTPException
from typing import Dict
from pydantic import BaseModel
from google.cloud import exceptions
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timedelta
from typing import Union
from pydantic import Field
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.openapi.models import OAuthFlows as OAuthFlowsModel
from fastapi.openapi.models import OAuthFlowPassword
from fastapi.security.oauth2 import OAuth2
from pydantic import BaseModel
from jose import JWTError, jwt
from passlib.context import CryptContext
import datetime
import pyfiglet
from ..api import get_current_user, db, app

router = APIRouter()


class TopicActivityInterval(str, Enum):
    yearly = "yearly"
    monthly = "monthly"
    weekly = "weekly"
    daily = "daily"


@router.get(
    "/protocols/{protocol_name}/discourse-topic-activity",
    tags=["Discourse - Ecosystem"],
    dependencies=[Depends(get_current_user)],
    responses={
        200: {
            "description": "Discourse Topic Activity",
            "content": {
                "application/json": {
                    "example": {
                        "xAxis": {
                            "type": "category",
                            "data": [
                                "2020-12-31",
                                "2021-12-31",
                                "2022-12-31",
                                "2023-12-31",
                            ],
                        },
                        "yAxis": {"type": "value"},
                        "series": [{"data": [128, 535, 246, 100], "type": "line"}],
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
            "content": {"application/json": {"example": {"error": "Error description"}}},
        },
    },
)
def discourse_topic_activity(
    protocol_name: str = Path(..., description="Protocol name"),
    interval: TopicActivityInterval = Query(..., description="Interval"),
):
    """
    Returns the discourse topic activity for the protocol.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-discourse").document(f"topic_activity").get(field_paths=["data"]).to_dict()
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

    df = pd.DataFrame(list(data.items()), columns=["Date", "Count"])
    df["Date"] = pd.to_datetime(df["Date"])

    # Set the date as index
    df.set_index("Date", inplace=True)

    # Aggregate the data to different time intervals
    if interval == TopicActivityInterval.yearly:
        data_interval = df.resample("Y").sum()

    elif interval == TopicActivityInterval.monthly:
        data_interval = df.resample("M").sum()

    elif interval == TopicActivityInterval.weekly:
        data_interval = df.resample("W").sum()

    elif interval == TopicActivityInterval.daily:
        data_interval = df.resample("D").sum()

    else:
        raise HTTPException(status_code=400, detail="Invalid interval.")

    if data_interval.empty:
        raise HTTPException(status_code=204, detail="Content is empty.")

    # Create list of dictionaries
    xAxis_data = [idx.strftime("%Y-%m-%d") for idx in data_interval.index]
    series_data = [int(row.Count) for row in data_interval.itertuples()]

    echarts_options = {
        "xAxis": {
            "type": "category",
            "data": xAxis_data,
        },
        "yAxis": {"type": "value"},
        "series": [{"data": series_data, "type": "line"}],
    }
    return echarts_options


@router.get(
    "/protocols/{protocol_name}/discourse-topic-metrics",
    tags=["Discourse - Ecosystem"],
    dependencies=[Depends(get_current_user)],
    responses={
        200: {
            "description": "Discourse Topic Metrics",
            "content": {
                "application/json": {
                    "example": {
                        "average_replies_per_topic": 0.96,
                        "total_topics": 1009,
                        "total_posts": 3624,
                        "average_views_per_topic": 396.44,
                        "average_post_per_topic": 3.59,
                        "average_likes_per_topic": 1.77,
                        "total_replies": 973,
                        "total_views": 400010,
                        "total_likes": 1790,
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
            "content": {"application/json": {"example": {"error": "Error description"}}},
        },
    },
)
def discourse_topic_metrics(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the discourse topic metrics for the protocol.

    """

    try:
        ref = db.collection(f"{protocol_name}-discourse").document(f"topic_metrics").get(field_paths=["data"]).to_dict()

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
    "/protocols/{protocol_name}/discourse-user-metrics",
    tags=["Discourse - Ecosystem"],
    dependencies=[Depends(get_current_user)],
    responses={
        200: {
            "description": "Discourse User Metrics",
            "content": {
                "application/json": {
                    "example": {
                        "users_average_topics_entered": 0.67,
                        "users_average_post_count": 1.79,
                        "users_total_days_visited": 16780,
                        "users_total_topic_count": 1012,
                        "users_total_likes_given": 2000,
                        "users_average_posts_read": 52.82,
                        "users_total_posts_read": 80284,
                        "users_total_topics_entered": 1012,
                        "users_average_days_visited": 11.04,
                        "users_average_likes_received": 1.31,
                        "users_average_topic_count": 0.67,
                        "users_total_post_count": 2714,
                        "total_users": 1520,
                        "users_total_likes_received": 1985,
                        "users_average_likes_given": 1.32,
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
            "content": {"application/json": {"example": {"error": "Error description"}}},
        },
    },
)
def discourse_user_metrics(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the discourse user metrics for the protocol.

    """

    try:
        ref = db.collection(f"{protocol_name}-discourse").document(f"user_metrics").get(field_paths=["data"]).to_dict()

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
    "/protocols/{protocol_name}/discourse-categories",
    tags=["Discourse - Ecosystem"],
    dependencies=[Depends(get_current_user)],
    responses={
        200: {
            "description": "Discourse Categories",
            "content": {
                "application/json": {
                    "example": {
                        "categories": [
                            {
                                "color": "26ff76",
                                "topics_year": 12,
                                "topics_month": 0,
                                "description_text": "If you are new to Flow introduce yourself here!",
                                "topic_count": 102,
                                "topics_all_time": 102,
                                "topics_week": 0,
                                "num_featured_topics": 10,
                                "topics_day": 0,
                                "name": "New To Flow",
                                "id": 5,
                                "post_count": 361,
                                "subcategories": [],
                                "slug": "new-to-flow",
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
            "content": {"application/json": {"example": {"error": "Error description"}}},
        },
    },
)
def discourse_categories(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the discourse categories for the protocol.

    """

    try:
        ref = db.collection(f"{protocol_name}-discourse").document(f"categories").get(field_paths=["data"]).to_dict()

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
    "/protocols/{protocol_name}/discourse-tags",
    tags=["Discourse - Ecosystem"],
    dependencies=[Depends(get_current_user)],
    responses={
        200: {
            "description": "Discourse Tags",
            "content": {
                "application/json": {
                    "example": {
                        "tags": [
                            {
                                "count": 48,
                                "id": "flow-sdk-js",
                                "text": "flow-sdk-js",
                            },
                        ]
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
            "content": {"application/json": {"example": {"error": "Error description"}}},
        },
    },
)
def discourse_tags(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the discourse tags for the protocol.

    """

    try:
        ref = db.collection(f"{protocol_name}-discourse").document(f"tags").get(field_paths=["data"]).to_dict()

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


class DiscourseTopTopicsInterval(str, Enum):
    daily = "daily"
    weekly = "weekly"
    monthly = "monthly"
    quarterly = "quarterly"
    yearly = "yearly"
    all_time = "all"


@router.get(
    "/protocols/{protocol_name}/discourse-top-topics",
    tags=["Discourse - Ecosystem"],
    dependencies=[Depends(get_current_user)],
    responses={
        200: {
            "description": "Discourse Top Topics",
            "content": {
                "application/json": {
                    "example": [
                        {
                            "like_count": 7,
                            "highest_post_number": 13,
                            "posters_len": 5,
                            "created_at": "2023-07-11T03:42:30.051Z",
                            "id": 4984,
                            "title": "Bridging tokens to Flow",
                            "reply_count": 4,
                            "slug": "bridging-tokens-to-flow",
                            "views": 44,
                            "posts_count": 13,
                            "last_posted_at": "2023-07-14T23:12:08.302Z",
                        },
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
def discourse_top_topics(
    protocol_name: str = Path(..., description="Protocol name"),
    interval: DiscourseTopTopicsInterval = Query(..., description="Interval"),
):
    """
    Returns the discourse top topics for the protocol.

    """

    try:
        ref = db.collection(f"{protocol_name}-discourse").document(f"top_topics").get(field_paths=["data"]).to_dict()

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

    interval_data = None
    if interval == DiscourseTopTopicsInterval.daily:
        interval_data = data["daily"]

    elif interval == DiscourseTopTopicsInterval.weekly:
        interval_data = data["weekly"]

    elif interval == DiscourseTopTopicsInterval.monthly:
        interval_data = data["monthly"]

    elif interval == DiscourseTopTopicsInterval.quarterly:
        interval_data = data["quarterly"]

    elif interval == DiscourseTopTopicsInterval.yearly:
        interval_data = data["yearly"]

    elif interval == DiscourseTopTopicsInterval.all_time:
        interval_data = data["all"]

    if not interval_data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return interval_data


class LatestTopicsOrder(str, Enum):
    default = "default"
    created = "created"
    activity = "activity"
    views = "views"
    posts = "posts"
    likes = "likes"
    op_likes = "op_likes"


@router.get(
    "/protocols/{protocol_name}/discourse-latest-topics",
    tags=["Discourse - Ecosystem"],
    dependencies=[Depends(get_current_user)],
    responses={
        200: {
            "description": "Discourse Latest Topics",
            "content": {
                "application/json": {
                    "example": [
                        {
                            "like_count": 7,
                            "highest_post_number": 13,
                            "posters_len": 5,
                            "created_at": "2023-07-11T03:42:30.051Z",
                            "id": 4984,
                            "title": "Bridging tokens to Flow",
                            "reply_count": 4,
                            "slug": "bridging-tokens-to-flow",
                            "views": 44,
                            "posts_count": 13,
                            "last_posted_at": "2023-07-14T23:12:08.302Z",
                        },
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
def discourse_latest_topics(
    protocol_name: str = Path(..., description="Protocol name"),
    order: LatestTopicsOrder = Query(..., description="Order"),
):
    """
    Returns the discourse latest topics for the protocol.

    """

    try:
        ref = db.collection(f"{protocol_name}-discourse").document(f"latest_topics").get(field_paths=["data"]).to_dict()

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

    order_data = None
    if order == LatestTopicsOrder.default:
        order_data = data["default"]

    elif order == LatestTopicsOrder.created:
        order_data = data["created"]

    elif order == LatestTopicsOrder.activity:
        order_data = data["activity"]

    elif order == LatestTopicsOrder.views:
        order_data = data["views"]

    elif order == LatestTopicsOrder.posts:
        order_data = data["posts"]

    elif order == LatestTopicsOrder.likes:
        order_data = data["likes"]

    elif order == LatestTopicsOrder.op_likes:
        order_data = data["op_likes"]

    if not order_data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return order_data


@router.get(
    "/protocols/{protocol_name}/discourse-latest-posts",
    tags=["Discourse - Ecosystem"],
    dependencies=[Depends(get_current_user)],
    responses={
        200: {
            "description": "Discourse Latest Posts",
            "content": {
                "application/json": {
                    "example": [
                        {
                            "reads": 3,
                            "created_at": "2023-07-14T23:12:08.302Z",
                            "reply_count": 0,
                            "score": 0.6,
                            "updated_at": "2023-07-14T23:12:08.302Z",
                            "category_id": 37,
                            "user_id": 455,
                            "topic_title": "Bridging tokens to Flow",
                            "name": "",
                            "id": 9669,
                            "topic_id": 4984,
                            "quote_count": 0,
                            "readers_count": 2,
                            "avatar_template": "/user_avatar/forum.onflow.org/sipmarch7/{size}/989_2.png",
                            "topic_slug": "bridging-tokens-to-flow",
                            "username": "sipmarch7",
                        },
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
def discourse_latest_posts(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the discourse latest posts for the protocol.

    """

    try:
        ref = db.collection(f"{protocol_name}-discourse").document(f"latest_posts").get(field_paths=["data"]).to_dict()

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


class TopUsersInterval(str, Enum):
    all_time = "all"
    yearly = "yearly"
    quarterly = "quarterly"
    monthly = "monthly"
    weekly = "weekly"
    daily = "daily"


class TopUsersOrder(str, Enum):
    likes_received = "likes_received"
    likes_given = "likes_given"
    topic_count = "topic_count"
    post_count = "post_count"
    topic_entered = "topic_entered"
    posts_read = "posts_read"
    days_visited = "days_visited"


@router.get(
    "/protocols/{protocol_name}/discourse-top-users",
    tags=["Discourse - Ecosystem"],
    dependencies=[Depends(get_current_user)],
    responses={
        200: {
            "description": "Discourse Top Users",
            "content": {
                "application/json": {
                    "example": [
                        {
                            "days_visited": 30,
                            "user_id": 2096,
                            "posts_read": 10,
                            "name": "Adam",
                            "topics_entered": 5,
                            "id": 2096,
                            "likes_given": 0,
                            "post_count": 0,
                            "likes_received": 0,
                            "topic_count": 0,
                            "avatar_template": "/letter_avatar_proxy/v4/letter/a/c0e974/{size}.png",
                            "username": "AdamRay",
                        },
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
def discourse_top_users(
    protocol_name: str = Path(..., description="Protocol name"),
    interval: TopUsersInterval = Query(..., description="Interval"),
    order: TopUsersOrder = Query(..., description="Order"),
):
    """
    Returns the discourse top users for the protocol. It will only list directory users. It doesn't necessarily include all users.
    It typically only includes users who have been active in some way, i.e., those who have posted, liked something, or otherwise interacted with the forum.
    If a user account has no activity, it may not be listed in this endpoint.

    """

    try:
        ref = db.collection(f"{protocol_name}-discourse").document(f"top_users").get(field_paths=["data"]).to_dict()

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

    interval_data = None
    if interval == TopUsersInterval.all_time:
        interval_data = data["all"]

    elif interval == TopUsersInterval.yearly:
        interval_data = data["yearly"]

    elif interval == TopUsersInterval.quarterly:
        interval_data = data["quarterly"]

    elif interval == TopUsersInterval.monthly:
        interval_data = data["monthly"]

    elif interval == TopUsersInterval.weekly:
        interval_data = data["weekly"]

    elif interval == TopUsersInterval.daily:
        interval_data = data["daily"]

    if not interval_data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    order_data = None
    if order == TopUsersOrder.likes_received:
        order_data = interval_data["likes_received"]

    elif order == TopUsersOrder.likes_given:
        order_data = interval_data["likes_given"]

    elif order == TopUsersOrder.topic_count:
        order_data = interval_data["topic_count"]

    elif order == TopUsersOrder.post_count:
        order_data = interval_data["post_count"]

    elif order == TopUsersOrder.topic_entered:
        order_data = interval_data["topic_entered"]

    elif order == TopUsersOrder.posts_read:
        order_data = interval_data["posts_read"]

    elif order == TopUsersOrder.days_visited:
        order_data = interval_data["days_visited"]

    if not order_data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return order_data
