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


@router.get(
    "/{protocol_name}/repository-info",
    dependencies=[Depends(get_current_user)],
    tags=["Github - Project"],
    responses={
        200: {
            "description": "Repository information",
            "content": {
                "application/json": {
                    "example": {
                        "fork_count": 1207,
                        "is_fork": False,
                        "watcher_count": 102,
                        "issue_count": 1122,
                        "commit_comment_count": 4645,
                        "release_count": 15,
                        "owner_avatar_url": "https://avatars.githubusercontent.com/u/103585522?v=4",
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
                        "pull_request_count": 2107,
                        "created_at": "2022-03-19T15:01:46Z",
                        "description": "Lenster is a decentralized and permissionless social media app built with Lens Protocol 🌿",
                        "owner_login": "lensterxyz",
                        "primary_language_color": "#3178c6",
                        "stargazer_count": 20601,
                        "environment_count": 8,
                        "primary_language_name": "TypeScript",
                        "url": "https://github.com/lensterxyz/lenster",
                        "default_branch_commit_count": 6405,
                        "is_archived": False,
                        "updated_at": "2023-07-18T12:39:12Z",
                        "disk_usage": 30200,
                        "is_empty": False,
                        "owner": "lensterxyz",
                        "repo": "lenster",
                    }
                }
            },
        },
        204: {
            "description": "No content found",
            "content": {"application/json": {"example": None}},
        },
        404: {
            "description": "Not found",
            "content": {"application/json": {"example": {"error": "Error description"}}},
        },
    },
)
def repository_info(
    protocol_name: str = Path(..., description="Protocol name"),
    owner: str = Query(..., description="Project owner name"),
    repo: str = Query(..., description="Project repository name"),
):
    """
    Returns the last year of commit activity grouped by week. The days array is a group of commits per day, starting on Sunday.



    """

    try:
        ref = (
            db.collection(f"{protocol_name}-widgets")
            .document("repositories")
            .collection(f"{owner}#{repo}")
            .document("repository_info")
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

    data.update({"owner": owner, "repo": repo})
    return data


@router.get(
    "/{protocol_name}/health-score",
    dependencies=[Depends(get_current_user)],
    tags=["Github - Project"],
    responses={
        200: {
            "description": "Repository information",
            "content": {
                "application/json": {
                    "example": {
                        "commit_activity": 54.01,
                        "contribution_activity": 0.59,
                        "issue_activity": 67.87,
                        "pull_request_activity": 8.32,
                        "release_activity": 21,
                    }
                }
            },
        },
        204: {
            "description": "No content found",
            "content": {"application/json": {"example": None}},
        },
        404: {
            "description": "Not found",
            "content": {"application/json": {"example": {"error": "Error description"}}},
        },
    },
)
def health_score(
    protocol_name: str = Path(..., description="Protocol name"),
    owner: str = Query(..., description="Project owner name"),
    repo: str = Query(..., description="Project repository name"),
):
    """
    Returns the health score of a project. The health score is calculated based on the following metrics:

    - `commit_activity`: A high score suggests frequent and recent commit activity. A low score may indicate infrequent or old commit activity.

    - `issue_activity`: A high score indicates efficient issue management, such as closing issues quickly and getting many comments. A low score may suggest poor issue handling.

    - `pull_request_activity`: A high score indicates effective pull request management, like quick closing times and receiving many comments. A low score suggests the opposite.

    - `release_activity`: A high score represents frequent and recent software releases. A low score may imply less frequent or outdated releases.

    - `contribution_activity`: A high score indicates a healthy number of contributors with increasing commit trends. A low score may suggest a lack of contributors or decreasing commit trends.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-widgets")
            .document("repositories")
            .collection(f"{owner}#{repo}")
            .document("health_score")
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
    tags=["Github - Project"],
    responses={
        200: {
            "description": "Commit activity",
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
            "description": "No content found",
            "content": {"application/json": {"example": None}},
        },
        404: {
            "description": "Not found",
            "content": {"application/json": {"example": {"error": "Error description"}}},
        },
    },
)
def commit_activity(
    protocol_name: str = Path(..., description="Protocol name"),
    owner: str = Query(..., description="Project owner name"),
    repo: str = Query(..., description="Project repository name"),
):
    """
    Returns the last year of commit activity grouped by week. The days array is a group of commits per day, starting on Sunday.


    **_NOTE:_**  Alias for [stats/commit_activity](https://docs.github.com/en/rest/metrics/statistics?apiVersion=2022-11-28#get-the-weekly-commit-activity) endpoint.
    """

    try:
        ref = (
            db.collection(f"{protocol_name}-widgets")
            .document("repositories")
            .collection(f"{owner}#{repo}")
            .document("commit_activity")
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
    tags=["Github - Project"],
    responses={
        200: {
            "description": "Participation",
            "content": {
                "application/json": {
                    "example": {
                        "xAxis": {
                            "type": "category",
                            "data": ["2022-07-26", "2022-08-02", "2022-08-09"],
                        },
                        "yAxis": {"type": "value"},
                        "series": [
                            {"name": "All", "data": [3, 5, 7], "type": "line"},
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
                }
            },
        },
        204: {
            "description": "No content found",
            "content": {"application/json": {"example": None}},
        },
        404: {
            "description": "Not found",
            "content": {"application/json": {"example": {"error": "Error description"}}},
        },
    },
)
def participation(
    protocol_name: str = Path(..., description="Protocol name"),
    owner: str = Query(..., description="Project owner name"),
    repo: str = Query(..., description="Project repository name"),
):
    """
    Returns the participation data in Apache e-chart format that has total commit counts for the owner, others and all (owner + others) in the last 52 weeks. The array order is oldest week (index 0) to most recent week. The most recent week is seven days ago at UTC midnight to today at UTC midnight.


    **_NOTE:_**  Wrapper for [stats/participation](https://docs.github.com/en/rest/metrics/statistics?apiVersion=2022-11-28#get-the-weekly-commit-count) endpoint.


    """

    try:
        ref = (
            db.collection(f"{protocol_name}-widgets")
            .document("repositories")
            .collection(f"{owner}#{repo}")
            .document("participation")
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
    "/{protocol_name}/participation_count",
    dependencies=[Depends(get_current_user)],
    tags=["Github - Project"],
    responses={
        200: {
            "description": "Participation Count",
            "content": {
                "application/json": {
                    "example": {
                        "owner": 24,
                        "all": 12,
                    }
                }
            },
        },
        204: {
            "description": "No content found",
            "content": {"application/json": {"example": None}},
        },
        404: {
            "description": "Not found",
            "content": {"application/json": {"example": {"error": "Error description"}}},
        },
    },
)
def participation_count(
    protocol_name: str = Path(..., description="Protocol name"),
    owner: str = Query(..., description="Project owner name"),
    repo: str = Query(..., description="Project repository name"),
):
    """
    Returns the participation count data of last 52 weeks for the owner and all.


    """

    try:
        ref = (
            db.collection(f"{protocol_name}-widgets")
            .document("repositories")
            .collection(f"{owner}#{repo}")
            .document("participation_count")
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
    tags=["Github - Project"],
    responses={
        200: {
            "description": "Code Frequency",
            "content": {
                "application/json": {
                    "example": {
                        "xAxis": {"data": ["2022-08-28", "2022-09-04", "2022-09-11"]},
                        "yAxis": {},
                        "series": [
                            {
                                "data": [785, 12, 452],
                                "type": "line",
                                "stack": "x",
                            },
                            {
                                "data": [0, 0, -123],
                                "type": "line",
                                "stack": "x",
                            },
                        ],
                    }
                }
            },
        },
        204: {
            "description": "No content found",
            "content": {"application/json": {"example": None}},
        },
        404: {
            "description": "Not found",
            "content": {"application/json": {"example": {"error": "Error description"}}},
        },
    },
)
def code_frequency(
    protocol_name: str = Path(..., description="Protocol name"),
    owner: str = Query(..., description="Project owner name"),
    repo: str = Query(..., description="Project repository name"),
):
    """
    Returns the code frequency data in Apache e-chart format that has weekly aggregate of the number of additions and deletions pushed to a repository.


    **_NOTE:_**  Wrapper for [stats/code_frequency](https://docs.github.com/en/rest/metrics/statistics?apiVersion=2022-11-28#get-the-weekly-commit-count) endpoint.
    """

    try:
        ref = (
            db.collection(f"{protocol_name}-widgets")
            .document("repositories")
            .collection(f"{owner}#{repo}")
            .document("code_frequency")
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
    tags=["Github - Project"],
    responses={
        200: {
            "description": "Punch Card",
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
            "description": "No content found",
            "content": {"application/json": {"example": None}},
        },
        404: {
            "description": "Not found",
            "content": {"application/json": {"example": {"error": "Error description"}}},
        },
    },
)
def punch_card(
    protocol_name: str = Path(..., description="Protocol name"),
    owner: str = Query(..., description="Project owner name"),
    repo: str = Query(..., description="Project repository name"),
):
    """
    Returns the punch card data.

    Each object contains the day number, hour number, and number of commits:

    0-6: Sunday - Saturday
    0-23: Hour of day
    Number of commits
    For example, `{'day':2, 'hour':14, 'commits':25 }` indicates that there were 25 total commits, during the 2:00pm hour on Tuesdays. All times are based on the time zone of individual commits.


    **_NOTE:_**  Wrapper for [stats/punch_card](https://docs.github.com/en/rest/metrics/statistics?apiVersion=2022-11-28#get-the-hourly-commit-count-for-each-day) endpoint.
    """

    try:
        ref = (
            db.collection(f"{protocol_name}-widgets")
            .document("repositories")
            .collection(f"{owner}#{repo}")
            .document("punch_card")
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
    tags=["Github - Project"],
    responses={
        200: {
            "description": "Contributors",
            "content": {
                "application/json": {
                    "example": {
                        "total": 4617,
                        "weeks": [
                            {"w": 1647129600, "a": 20478, "d": 656, "c": 82},
                        ],
                        "author": {
                            "login": "bigint",
                            "id": 69431456,
                            "node_id": "MDQ6VXNlcjY5NDMxNDU2",
                            "avatar_url": "https://avatars.githubusercontent.com/u/69431456?v=4",
                            "gravatar_id": "",
                            "url": "https://api.github.com/users/bigint",
                            "html_url": "https://github.com/bigint",
                            "followers_url": "https://api.github.com/users/bigint/followers",
                            "following_url": "https://api.github.com/users/bigint/following{/other_user}",
                            "gists_url": "https://api.github.com/users/bigint/gists{/gist_id}",
                            "starred_url": "https://api.github.com/users/bigint/starred{/owner}{/repo}",
                            "subscriptions_url": "https://api.github.com/users/bigint/subscriptions",
                            "organizations_url": "https://api.github.com/users/bigint/orgs",
                            "repos_url": "https://api.github.com/users/bigint/repos",
                            "events_url": "https://api.github.com/users/bigint/events{/privacy}",
                            "received_events_url": "https://api.github.com/users/bigint/received_events",
                            "type": "User",
                            "site_admin": False,
                        },
                    }
                }
            },
        },
        204: {
            "description": "No content found",
            "content": {"application/json": {"example": None}},
        },
        404: {
            "description": "Not found",
            "content": {"application/json": {"example": {"error": "Error description"}}},
        },
    },
)
def contributors(
    protocol_name: str = Path(..., description="Protocol name"),
    owner: str = Query(..., description="Project owner name"),
    repo: str = Query(..., description="Project repository name"),
):
    """
        Returns the total number of commits authored by the contributor. In addition, the response includes a Weekly Hash (weeks array) with the following information:

        * w - Start of the week, given as a Unix timestamp.
        * a - Number of additions
        * d - Number of deletions
        * c - Number of commits


    **_NOTE:_**  Wrapper for [stats/contributors](https://docs.github.com/en/rest/metrics/statistics?apiVersion=2022-11-28#get-all-contributor-commit-activity) endpoint.
    """

    try:
        # ref = db.collection(f'{protocol_name}-widgets').document('repositories').collection(
        #     f'{owner}#{repo}').document('contributors').get(field_paths=['data']).to_dict()

        # if ref is None:
        #     raise exceptions.NotFound('Collection or document not found')

        doc_base = db.collection(f"{protocol_name}-widgets").document("repositories").collection(f"{owner}#{repo}")

        # Initialize an empty list to hold all contributors
        data = []

        # Fetch all documents that start with 'contributors' in their name
        all_docs = doc_base.list_documents()
        for doc in all_docs:
            if "contributors" in doc.id:
                # Get 'data' field from each 'contributors' document and extend the all_contributors list
                doc_dict = doc.get().to_dict()
                if doc_dict and "data" in doc_dict:
                    data.extend(doc_dict["data"])

    except exceptions.NotFound as ex:
        # Handle case where document or collection does not exist
        raise HTTPException(status_code=404, detail=str(ex))

    except Exception as e:
        # Handle other exceptions
        raise HTTPException(status_code=500, detail=f"An error occurred {str(e)}")

    # data = ref.get('data', None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


@router.get(
    "/{protocol_name}/community-profile",
    dependencies=[Depends(get_current_user)],
    tags=["Github - Project"],
    responses={
        200: {
            "description": "Community Profile",
            "content": {
                "application/json": {
                    "example": {
                        "health_percentage": 100,
                        "description": "My first repository on GitHub!",
                        "documentation": None,
                        "files": {
                            "code_of_conduct": {
                                "name": "Contributor Covenant",
                                "key": "contributor_covenant",
                                "url": "https://api.github.com/codes_of_conduct/contributor_covenant",
                                "html_url": "https://github.com/octocat/Hello-World/blob/master/CODE_OF_CONDUCT.md",
                            },
                            "code_of_conduct_file": {
                                "url": "https://api.github.com/repos/octocat/Hello-World/contents/CODE_OF_CONDUCT.md",
                                "html_url": "https://github.com/octocat/Hello-World/blob/master/CODE_OF_CONDUCT.md",
                            },
                            "contributing": {
                                "url": "https://api.github.com/repos/octocat/Hello-World/contents/CONTRIBUTING",
                                "html_url": "https://github.com/octocat/Hello-World/blob/master/CONTRIBUTING",
                            },
                            "issue_template": {
                                "url": "https://api.github.com/repos/octocat/Hello-World/contents/ISSUE_TEMPLATE",
                                "html_url": "https://github.com/octocat/Hello-World/blob/master/ISSUE_TEMPLATE",
                            },
                            "pull_request_template": {
                                "url": "https://api.github.com/repos/octocat/Hello-World/contents/PULL_REQUEST_TEMPLATE",
                                "html_url": "https://github.com/octocat/Hello-World/blob/master/PULL_REQUEST_TEMPLATE",
                            },
                            "license": {
                                "name": "MIT License",
                                "key": "mit",
                                "spdx_id": "MIT",
                                "url": "https://api.github.com/licenses/mit",
                                "html_url": "https://github.com/octocat/Hello-World/blob/master/LICENSE",
                                "node_id": "MDc6TGljZW5zZW1pdA==",
                            },
                            "readme": {
                                "url": "https://api.github.com/repos/octocat/Hello-World/contents/README.md",
                                "html_url": "https://github.com/octocat/Hello-World/blob/master/README.md",
                            },
                        },
                        "updated_at": "2017-02-28T19:09:29Z",
                        "content_reports_enabled": None,
                    }
                }
            },
        },
        204: {
            "description": "No content found",
            "content": {"application/json": {"example": None}},
        },
        404: {
            "description": "Not found",
            "content": {"application/json": {"example": {"error": "Error description"}}},
        },
    },
)
def community_profile(
    protocol_name: str = Path(..., description="Protocol name"),
    owner: str = Query(..., description="Project owner name"),
    repo: str = Query(..., description="Project repository name"),
):
    """
    Returns all community profile metrics for a repository. The repository cannot be a fork.

    The returned metrics include an overall health score, the repository description, the presence of documentation, the detected code of conduct, the detected license, and the presence of ISSUE_TEMPLATE, PULL_REQUEST_TEMPLATE, README, and CONTRIBUTING files.

    The health_percentage score is defined as a percentage of how many of these four documents are present: README, CONTRIBUTING, LICENSE, and CODE_OF_CONDUCT. For example, if all four documents are present, then the health_percentage is 100. If only one is present, then the health_percentage is 25.

    content_reports_enabled is only returned for organization-owned repositories.


    **_NOTE:_**  Alias for [stats/community_profile](https://docs.github.com/en/rest/metrics/community?apiVersion=2022-11-28#get-community-profile-metrics) endpoint.
    """

    try:
        ref = (
            db.collection(f"{protocol_name}-widgets")
            .document("repositories")
            .collection(f"{owner}#{repo}")
            .document("community_profile")
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
    tags=["Github - Project"],
    responses={
        200: {
            "description": "Language breakdown",
            "content": {"application/json": {"example": [{"name": "JavaScript", "percentage": 100, "size": 55852}]}},
        },
        204: {
            "description": "No content found",
            "content": {"application/json": {"example": None}},
        },
        404: {
            "description": "Not found",
            "content": {"application/json": {"example": {"error": "Error description"}}},
        },
    },
)
def language_breakdown(
    protocol_name: str = Path(..., description="Protocol name"),
    owner: str = Query(..., description="Project owner name"),
    repo: str = Query(..., description="Project repository name"),
):
    """
    Returns the language breakdown for the repository.


    """

    try:
        ref = (
            db.collection(f"{protocol_name}-widgets")
            .document("repositories")
            .collection(f"{owner}#{repo}")
            .document("language_breakdown")
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
    tags=["Github - Project"],
    responses={
        200: {
            "description": "Issue count",
            "content": {
                "application/json": {
                    "example": {
                        "closed": 12,
                        "open": 2,
                        "average_days_to_close_issues": 23,
                    }
                }
            },
        },
        204: {
            "description": "No content found",
            "content": {"application/json": {"example": None}},
        },
        404: {
            "description": "Not found",
            "content": {"application/json": {"example": {"error": "Error description"}}},
        },
    },
)
def issue_count(
    protocol_name: str = Path(..., description="Protocol name"),
    owner: str = Query(..., description="Project owner name"),
    repo: str = Query(..., description="Project repository name"),
):
    """
    Returns the open/closed issue count for the repository.


    """

    try:
        ref = (
            db.collection(f"{protocol_name}-widgets")
            .document("repositories")
            .collection(f"{owner}#{repo}")
            .document("issue_count")
            .get(field_paths=["data"])
            .to_dict()
        )

        if ref is None:
            raise exceptions.NotFound("Collection or document not found")

        try:
            ref2 = (
                db.collection(f"{protocol_name}-widgets")
                .document("repositories")
                .collection(f"{owner}#{repo}")
                .document("average_days_to_close_issues")
                .get(field_paths=["data"])
                .to_dict()
            )

        except Exception as ex:
            pass

    except exceptions.NotFound as ex:
        # Handle case where document or collection does not exist
        raise HTTPException(status_code=404, detail=str(ex))

    except Exception as e:
        # Handle other exceptions
        raise HTTPException(status_code=500, detail=f"An error occurred {str(e)}")

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    if ref2:
        data["average_days_to_close_issues"] = ref2.get("data", 0)

    return data


class MostActiveIssuesInterval(str, Enum):
    day = "day"
    week = "week"
    month = "month"
    year = "year"


@router.get(
    "/{protocol_name}/most-active-issues",
    dependencies=[Depends(get_current_user)],
    tags=["Github - Project"],
    responses={
        200: {
            "description": "Most active issues",
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
            "description": "No content found",
            "content": {"application/json": {"example": None}},
        },
        404: {
            "description": "Not found",
            "content": {"application/json": {"example": {"error": "Error description"}}},
        },
    },
)
def most_active_issues(
    protocol_name: str = Path(..., description="Protocol name"),
    owner: str = Query(..., description="Project owner name"),
    repo: str = Query(..., description="Project repository name"),
    interval: MostActiveIssuesInterval = Query(
        ...,
        description="Interval for which the most active issues are to be returned",
    ),
):
    """
    Returns the most active issues for the repository.

    interval can be one of the following: day, week, month, year


    """

    try:
        ref = (
            db.collection(f"{protocol_name}-widgets")
            .document("repositories")
            .collection(f"{owner}#{repo}")
            .document("most_active_issues")
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

    interval_data = data.get(interval.value, [])
    if not interval_data:
        raise HTTPException(status_code=204, detail="Content is empty for the given interval.")

    return interval_data


@router.get(
    "/{protocol_name}/pull-request-count",
    dependencies=[Depends(get_current_user)],
    tags=["Github - Project"],
    responses={
        200: {
            "description": "Pull request count",
            "content": {
                "application/json": {
                    "example": {
                        "closed": 12,
                        "open": 2,
                        "average_days_to_close_pull_requests": 23,
                    }
                }
            },
        },
        204: {
            "description": "No content found",
            "content": {"application/json": {"example": None}},
        },
        404: {
            "description": "Not found",
            "content": {"application/json": {"example": {"error": "Error description"}}},
        },
    },
)
def pull_request_count(
    protocol_name: str = Path(..., description="Protocol name"),
    owner: str = Query(..., description="Project owner name"),
    repo: str = Query(..., description="Project repository name"),
):
    """
    Returns the pull request count for the repository.


    """

    try:
        ref = (
            db.collection(f"{protocol_name}-widgets")
            .document("repositories")
            .collection(f"{owner}#{repo}")
            .document("pull_request_count")
            .get(field_paths=["data"])
            .to_dict()
        )

        if ref is None:
            raise exceptions.NotFound("Collection or document not found")

        try:
            ref2 = (
                db.collection(f"{protocol_name}-widgets")
                .document("repositories")
                .collection(f"{owner}#{repo}")
                .document("average_days_to_close_pull_requests")
                .get(field_paths=["data"])
                .to_dict()
            )

        except Exception as ex:
            pass

    except exceptions.NotFound as ex:
        # Handle case where document or collection does not exist
        raise HTTPException(status_code=404, detail=str(ex))

    except Exception as e:
        # Handle other exceptions
        raise HTTPException(status_code=500, detail=f"An error occurred {str(e)}")

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    if ref2:
        data["average_days_to_close_pull_requests"] = ref2.get("data", 0)

    return data


class RecentIssuesOrder(str, Enum):
    created_at = "created_at"
    updated_at = "updated_at"


@router.get(
    "/{protocol_name}/recent-issues",
    dependencies=[Depends(get_current_user)],
    tags=["Github - Project"],
    responses={
        200: {
            "description": "Recent issues",
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
                            "url": "https://www.github.com/lensterxyz/lenster/issues/3213",
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
            "description": "No content found",
            "content": {"application/json": {"example": None}},
        },
        404: {
            "description": "Not found",
            "content": {"application/json": {"example": {"error": "Error description"}}},
        },
    },
)
def recent_issues(
    protocol_name: str = Path(..., description="Protocol name"),
    owner: str = Query(..., description="Project owner name"),
    repo: str = Query(..., description="Project repository name"),
    order_by: RecentIssuesOrder = Query(RecentIssuesOrder.created_at, description="Order by field"),
):
    """
    Returns the recent issues for the repository ordered by 'created_at' or 'updated_at'.
    Args:
        protocol_name (str): Protocol name.
        owner (str): Project owner name.
        repo (str): Project repository name.
        order_by (RecentIssuesOrder): Field by which to order the issues. Can be one of the following values: created_at, updated_at.
    """

    field_name = None
    if order_by == RecentIssuesOrder.created_at:
        field_name = "recent_created_issues"

    elif order_by == RecentIssuesOrder.updated_at:
        field_name = f"recent_updated_issues"

    try:
        collection_ref = (
            db.collection(f"{protocol_name}-widgets")
            .document("repositories")
            .collection(f"{owner}#{repo}")
            .document(field_name)
            .get(field_paths=["data"])
            .to_dict()
        )

        if collection_ref is None:
            raise HTTPException(status_code=404, detail="Collection or document not found.")

    except exceptions.NotFound as ex:
        # Handle case where document or collection does not exist
        raise HTTPException(status_code=404, detail=str(ex))

    except Exception as e:
        # Handle other exceptions
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

    data = collection_ref.get("data", None)

    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


class RecentPullRequestOrder(str, Enum):
    created_at = "created_at"
    updated_at = "updated_at"


@router.get(
    "/{protocol_name}/recent-pull-requests",
    dependencies=[Depends(get_current_user)],
    tags=["Github - Project"],
    responses={
        200: {
            "description": "Recent pull requests",
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
                            "url": "https://www.github.com/lensterxyz/lenster/pull/3212",
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
            "description": "No content found",
            "content": {"application/json": {"example": None}},
        },
        404: {
            "description": "Not found",
            "content": {"application/json": {"example": {"error": "Error description"}}},
        },
    },
)
def recent_pull_requests(
    protocol_name: str = Path(..., description="Protocol name"),
    owner: str = Query(..., description="Project owner name"),
    repo: str = Query(..., description="Project repository name"),
    order_by: RecentPullRequestOrder = Query(..., description="Order by field"),
):
    """
    Returns the recent pull requests for the repository.

    """

    field_name = None
    if order_by == RecentPullRequestOrder.created_at:
        field_name = "recent_created_pull_requests"

    elif order_by == RecentPullRequestOrder.updated_at:
        field_name = f"recent_updated_pull_requests"

    try:
        collection_ref = (
            db.collection(f"{protocol_name}-widgets")
            .document("repositories")
            .collection(f"{owner}#{repo}")
            .document(field_name)
            .get(field_paths=["data"])
            .to_dict()
        )

        if collection_ref is None:
            raise exceptions.NotFound("Collection or document not found")

    except exceptions.NotFound as ex:
        # Handle case where document or collection does not exist
        raise HTTPException(status_code=404, detail=str(ex))

    except Exception as e:
        # Handle other exceptions
        raise HTTPException(status_code=500, detail=f"An error occurred {str(e)}")

    data = collection_ref.get("data", None)

    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


@router.get(
    "/{protocol_name}/recent-stargazing-activity",
    dependencies=[Depends(get_current_user)],
    tags=["Github - Project"],
    responses={
        200: {
            "description": "Recent stargazing activity",
            "content": {
                "application/json": {
                    "example": {
                        "xAxis": {
                            "data": [91, 135, 143],
                            "yAxis": {},
                            "series": [
                                {
                                    "data": [
                                        "2023-04-16",
                                        "2023-04-23",
                                        "2023-04-30",
                                    ],
                                    "type": "line",
                                    "stack": "x",
                                },
                            ],
                        }
                    }
                }
            },
        },
        204: {
            "description": "No content found",
            "content": {"application/json": {"example": None}},
        },
        404: {
            "description": "Not found",
            "content": {"application/json": {"example": {"error": "Error description"}}},
        },
    },
)
def recent_stargazing_activity(
    protocol_name: str = Path(..., description="Protocol name"),
    owner: str = Query(..., description="Project owner name"),
    repo: str = Query(..., description="Project repository name"),
):
    """
    Returns the recent stargazing activity in Apache e-chart format. Fetches max_fetch_page (currently 15) * 100 stargazers and if last star date - first star date is more than 6 months, then chart interval will be month based. If less than 6 months and more than 1 month, then chart interval will be week based. If less than 1 month, then chart interval will be day based.


    """

    try:
        ref = (
            db.collection(f"{protocol_name}-widgets")
            .document("repositories")
            .collection(f"{owner}#{repo}")
            .document("recent_stargazing_activity")
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
    if (not data) or (not data["series"][0]["data"]):
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


class IssueActivityInterval(str, Enum):
    week = "week"
    month = "month"
    year = "year"


@router.get(
    "/{protocol_name}/issue-activity",
    tags=["Github - Project"],
    dependencies=[Depends(get_current_user)],
    responses={
        200: {
            "description": "Issue activity",
            "content": {
                "application/json": {
                    "example": {
                        "xAxis": {"data": ["2022-12-31", "2023-12-31"]},
                        "series": [
                            {"name": "New", "data": [442, 680]},
                            {"name": "Closed", "data": [331, 638]},
                        ],
                    }
                }
            },
        },
        204: {
            "description": "No content found",
            "content": {"application/json": {"example": None}},
        },
        404: {
            "description": "Not found",
            "content": {"application/json": {"example": {"error": "Error description"}}},
        },
    },
)
def issue_activity(
    protocol_name: str = Path(..., description="Protocol name"),
    owner: str = Query(..., description="Project owner name"),
    repo: str = Query(..., description="Project repository name"),
    interval: IssueActivityInterval = Query(..., description="Interval"),
):
    """
    Returns the issue activity in Apache e-chart format based on interval.


    It gives answers to questions like 'How many issues were opened and closed in specified interval?'.
    """

    try:
        ref = (
            db.collection(f"{protocol_name}-widgets")
            .document("repositories")
            .collection(f"{owner}#{repo}")
            .document(f"issue_chart_{interval.value}")
            .get(field_paths=["data"])
            .to_dict()
        )

        if ref is None:
            raise exceptions.NotFound("Collection or document not found")

        # # Initialize the base reference
        # doc_base = db.collection(f'{protocol_name}-widgets').document('repositories').collection(
        #     f'{owner}#{repo}')

        # # Initialize an empty list to hold all contributors
        # data = []

        # # Fetch all documents that start with 'contributors' in their name
        # all_docs = doc_base.list_documents()
        # for doc in all_docs:
        #     if 'issue_activity' in doc.id:
        #         # Get 'data' field from each 'contributors' document and extend the all_contributors list
        #         doc_dict = doc.get().to_dict()
        #         if doc_dict and 'data' in doc_dict:
        #             data.extend(doc_dict['data'])

    except exceptions.NotFound as ex:
        # Handle case where document or collection does not exist
        raise HTTPException(status_code=404, detail=str(ex))

    except Exception as e:
        # Handle other exceptions
        raise HTTPException(status_code=500, detail=f"An error occurred {str(e)}")

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    # process here
    # try:
    #     # Process data
    #     issues_df = pd.DataFrame(data)

    #     # Convert createdAt and closedAt to datetime format
    #     issues_df['createdAt'] = pd.to_datetime(issues_df['createdAt'])
    #     issues_df['closedAt'] = pd.to_datetime(issues_df['closedAt'])

    #     pd_interval = None
    #     if interval.value == 'week':
    #         pd_interval = 'W'

    #     elif interval.value == 'month':
    #         pd_interval = 'M'

    #     elif interval.value == 'year':
    #         pd_interval = 'Y'

    #     # Group by date and count new and closed issues separately
    #     new_issues = issues_df.resample(pd_interval, on='createdAt').size()
    #     closed_issues = issues_df[issues_df['closed']].resample(
    #         pd_interval, on='closedAt').size()

    #     # Ensure new_issues and closed_issues have the same index
    #     all_dates = new_issues.index.union(closed_issues.index)
    #     new_issues = new_issues.reindex(all_dates, fill_value=0)
    #     closed_issues = closed_issues.reindex(all_dates, fill_value=0)

    #     # Convert data to ECharts format
    #     dates = all_dates.strftime('%Y-%m-%d').tolist()
    #     echart_data = {
    #         'xAxis': {
    #             'data': dates
    #         },
    #         'series': [
    #             {
    #                 'name': 'Opened',
    #                 'data': new_issues.tolist()
    #             },
    #             {
    #                 'name': 'Closed',
    #                 'data': closed_issues.tolist()
    #             }
    #         ]
    #     }

    # except Exception as e:
    #     # Handle exceptions during data processing
    #     raise HTTPException(
    #         status_code=500, detail=f"An error occurred during data processing: {str(e)}")

    return data


class PullRequestActivityInterval(str, Enum):
    week = "week"
    month = "month"
    year = "year"


@router.get(
    "/{protocol_name}/pull-request-activity",
    tags=["Github - Project"],
    dependencies=[Depends(get_current_user)],
    responses={
        200: {
            "description": "Pull request activity",
            "content": {
                "application/json": {
                    "example": {
                        "xAxis": {"data": ["2022-12-31", "2023-12-31"]},
                        "series": [
                            {"name": "New", "data": [442, 680]},
                            {"name": "Closed", "data": [331, 638]},
                        ],
                    }
                }
            },
        },
        204: {
            "description": "No content found",
            "content": {"application/json": {"example": None}},
        },
        404: {
            "description": "Not found",
            "content": {"application/json": {"example": {"error": "Error description"}}},
        },
    },
)
def pull_request_activity(
    protocol_name: str = Path(..., description="Protocol name"),
    owner: str = Query(..., description="Project owner name"),
    repo: str = Query(..., description="Project repository name"),
    interval: PullRequestActivityInterval = Query(..., description="Interval"),
):
    """
    Returns the pull request activity in Apache e-chart format based on interval.


    It gives answers to questions like 'How many pull requests were opened and closed in specified interval?'.
    """

    try:
        ref = (
            db.collection(f"{protocol_name}-widgets")
            .document("repositories")
            .collection(f"{owner}#{repo}")
            .document(f"pull_request_chart_{interval.value}")
            .get(field_paths=["data"])
            .to_dict()
        )

        if ref is None:
            raise exceptions.NotFound("Collection or document not found")

        # Initialize the base reference
        # doc_base = db.collection(f'{protocol_name}-widgets').document('repositories').collection(
        #     f'{owner}#{repo}')

        # # Initialize an empty list to hold all contributors
        # data = []

        # # Fetch all documents that start with 'contributors' in their name
        # all_docs = doc_base.list_documents()
        # for doc in all_docs:
        #     if 'pull_request_activity' in doc.id:
        #         # Get 'data' field from each 'contributors' document and extend the all_contributors list
        #         doc_dict = doc.get().to_dict()
        #         if doc_dict and 'data' in doc_dict:
        #             data.extend(doc_dict['data'])

    except exceptions.NotFound as ex:
        # Handle case where document or collection does not exist
        raise HTTPException(status_code=404, detail=str(ex))

    except Exception as e:
        # Handle other exceptions
        raise HTTPException(status_code=500, detail=f"An error occurred {str(e)}")

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    # process here
    # try:

    # Process data
    # pull_requests_df = pd.DataFrame(data)

    # # Convert createdAt and closedAt to datetime format
    # pull_requests_df['createdAt'] = pd.to_datetime(
    #     pull_requests_df['createdAt'])
    # pull_requests_df['closedAt'] = pd.to_datetime(
    #     pull_requests_df['closedAt'])

    # pd_interval = None
    # if interval.value == 'week':
    #     pd_interval = 'W'

    # elif interval.value == 'month':
    #     pd_interval = 'M'

    # elif interval.value == 'year':
    #     pd_interval = 'Y'

    # # Group by date and count new and closed issues separately
    # new_pull_requests = pull_requests_df.resample(
    #     pd_interval, on='createdAt').size()
    # closed_pull_requests = pull_requests_df[pull_requests_df['closed']].resample(
    #     pd_interval, on='closedAt').size()

    # # Ensure new_issues and closed_issues have the same index
    # all_dates = new_pull_requests.index.union(closed_pull_requests.index)
    # new_pull_requests = new_pull_requests.reindex(all_dates, fill_value=0)
    # closed_pull_requests = closed_pull_requests.reindex(
    #     all_dates, fill_value=0)

    # # Convert data to ECharts format
    # dates = all_dates.strftime('%Y-%m-%d').tolist()
    # echart_data = {
    #     'xAxis': {
    #         'data': dates
    #     },
    #     'series': [
    #         {
    #             'name': 'Opened',
    #             'data': new_pull_requests.tolist()
    #         },
    #         {
    #             'name': 'Closed',
    #             'data': closed_pull_requests.tolist()
    #         }
    #     ]
    # }

    # except Exception as e:
    #     # Handle exceptions during data processing
    #     raise HTTPException(
    #         status_code=500, detail=f"An error occurred during data processing: {str(e)}")

    return data


@router.get(
    "/{protocol_name}/recent-commits",
    dependencies=[Depends(get_current_user)],
    tags=["Github - Project"],
    responses={
        200: {
            "description": "Recent commits",
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
            "description": "No content found",
            "content": {"application/json": {"example": None}},
        },
        404: {
            "description": "Not found",
            "content": {"application/json": {"example": {"error": "Error description"}}},
        },
    },
)
def recent_commits(
    protocol_name: str = Path(..., description="Protocol name"),
    owner: str = Query(..., description="Project owner name"),
    repo: str = Query(..., description="Project repository name"),
):
    """
    Returns the recent commits for the repository. The author is selected as the one who has committed the code. Is the author is not available, then the committer is selected.


    """

    try:
        ref = (
            db.collection(f"{protocol_name}-widgets")
            .document("repositories")
            .collection(f"{owner}#{repo}")
            .document("recent_commits")
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
    tags=["Github - Project"],
    responses={
        200: {
            "description": "Recent releases",
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
            "content": {"application/json": {"example": {"error": "Error description"}}},
        },
    },
)
def recent_releases(
    protocol_name: str = Path(..., description="Protocol name"),
    owner: str = Query(..., description="Project owner name"),
    repo: str = Query(..., description="Project repository name"),
):
    """
    Returns the recent releases for the repository.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-widgets")
            .document("repositories")
            .collection(f"{owner}#{repo}")
            .document("recent_releases")
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

    return data[-10:]
