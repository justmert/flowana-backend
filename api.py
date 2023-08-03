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

load_dotenv()


tags_metadata = [
    {
        "name": "Github - Project",
        "description": "Github related endpoints for project",
    },
    {
        "name": "Github - Ecosystem",
        "description": "Github related endpoints for ecosystem",
    },
    {
        "name": "Discourse - Ecosystem",
        "description": "Discourse (Forum) related endpoints for ecosystem",
    },
    {
        "name": "Developers - Ecosystem",
        "description": "Developer related endpoints for ecosystem",
    },
    {
        "name": "Governance - Ecosystem",
        "description": "Governance related endpoints for ecosystem",
    },
    {
        "name": "Messari - Ecosystem",
        "description": "Messari asset related endpoints for ecosystem",
    },
    {"name": "Auth", "description": "Authentication related endpoints"},
]


admin_sdk_path = os.environ["FIREBASE_ADMIN_SDK_PATH"]
if not os.path.exists(admin_sdk_path):
    raise Exception(f"Admin SDK file not found in path {admin_sdk_path}")

cred = credentials.Certificate(os.environ["FIREBASE_ADMIN_SDK_PATH"])
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.environ[
    "FIREBASE_ADMIN_SDK_PATH"
]

app = firebase_admin.initialize_app(
    cred, {"projectId": os.environ["FIREBASE_PROJECT_ID"]}, name="flowana_api"
)

db = firestore.Client()
with open("protocols.json") as f:
    protocols = json.load(f)

SECRET_KEY = os.environ["API_SECRET_KEY"]
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_DAYS = 365

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = HTTPBearer()


class TokenData(BaseModel):
    username: str = None


def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password):
    return pwd_context.hash(password)


def create_access_token(data: dict, expires_delta: datetime.timedelta = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.datetime.utcnow() + expires_delta
    else:
        expire = datetime.datetime.utcnow() + datetime.timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


async def get_current_user(
    token: HTTPAuthorizationCredentials = Depends(oauth2_scheme),
):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(
            token.credentials, SECRET_KEY, algorithms=[ALGORITHM]
        )
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username)
    except JWTError:
        raise credentials_exception
    user = db.collection("users").document(username).get()
    if user:
        return user.to_dict()
    else:
        raise credentials_exception


app = FastAPI()


origins = [
    "https://192.168.1.4:8000",
    "https://localhost:3000",
    "http://localhost:3000",
    "http://localhost:3000/",
    "https://192.168.1.4",
    "http://localhost",
    "http://localhost:8080",
    "http://192.168.1.8:63157",
    "http://192.168.1.8",
    "https://flowana-dev.vercel.app"
    "http://flowana-dev.vercel.app"
    "http://www.flowana-dev.vercel.app"
    "https://www.flowana-dev.vercel.app",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class AdminIn(BaseModel):
    admin_username: str = Field(..., description="The username of the admin.")
    admin_password: str = Field(..., description="The password of the admin.")


class UserIn(BaseModel):
    username: str = Field(..., description="The username of the new user.")
    password: str = Field(..., description="The password of the new user.")


class UserToken(BaseModel):
    username: str
    access_token: str
    token_type: str


@app.get("/", dependencies=[Depends(get_current_user)], tags=["Auth"])
def read_root():
    return "Welcome to Flowana API!"


async def verify_admin_credentials(admin_in: AdminIn):
    if not (
        admin_in.admin_username == os.getenv("ADMIN_USERNAME")
        and verify_password(
            admin_in.admin_password,
            get_password_hash(os.getenv("ADMIN_PASSWORD")),
        )
    ):
        raise HTTPException(
            status_code=403,
            detail="Not enough permissions",
        )


class UserOut(BaseModel):
    username: str
    message: str


@app.post("/admin-create-user/", response_model=UserOut, tags=["Auth"])
async def admin_create_user(
    user_in: UserIn, admin_in: AdminIn = Depends(verify_admin_credentials)
):
    """
    Create a new user with admin credentials.
    """
    user = db.collection("users").document(user_in.username).get()

    if user.exists:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Username already exists",
        )

    hashed_password = get_password_hash(user_in.password)

    db.collection("users").document(user_in.username).set(
        {
            "username": user_in.username,
            "password": hashed_password,
        }
    )

    return UserOut(
        username=user_in.username, message="User successfully created"
    )


@app.post("/get-token/", response_model=UserToken, tags=["Auth"])
async def login_for_access_token(user_in: UserIn):
    user = db.collection("users").document(user_in.username).get().to_dict()
    if user:
        if verify_password(user_in.password, user.get("password")):
            access_token_expires = datetime.timedelta(
                days=ACCESS_TOKEN_EXPIRE_DAYS
            )
            access_token = create_access_token(
                data={"sub": user_in.username},
                expires_delta=access_token_expires,
            )
            return {
                "username": user_in.username,
                "access_token": access_token,
                "token_type": "bearer",
            }
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Incorrect username or password",
        headers={"WWW-Authenticate": "Bearer"},
    )


@app.get("/test-auth", dependencies=[Depends(get_current_user)], tags=["Auth"])
async def test_auth():
    return {"message": "You are authorized!"}


@app.get(
    "/protocols/{protocol_name}/repository-info",
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
            "content": {
                "application/json": {"example": {"error": "Error description"}}
            },
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    data.update({"owner": owner, "repo": repo})
    return data


@app.get(
    "/protocols/{protocol_name}/health-score",
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
            "content": {
                "application/json": {"example": {"error": "Error description"}}
            },
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


@app.get(
    "/protocols/{protocol_name}/commit-activity",
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
            "content": {
                "application/json": {"example": {"error": "Error description"}}
            },
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


@app.get(
    "/protocols/{protocol_name}/participation",
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
            "content": {
                "application/json": {"example": {"error": "Error description"}}
            },
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


@app.get(
    "/protocols/{protocol_name}/participation_count",
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
            "content": {
                "application/json": {"example": {"error": "Error description"}}
            },
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


@app.get(
    "/protocols/{protocol_name}/code-frequency",
    dependencies=[Depends(get_current_user)],
    tags=["Github - Project"],
    responses={
        200: {
            "description": "Code Frequency",
            "content": {
                "application/json": {
                    "example": {
                        "xAxis": {
                            "data": ["2022-08-28", "2022-09-04", "2022-09-11"]
                        },
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
            "content": {
                "application/json": {"example": {"error": "Error description"}}
            },
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


@app.get(
    "/protocols/{protocol_name}/punch-card",
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
            "content": {
                "application/json": {"example": {"error": "Error description"}}
            },
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


@app.get(
    "/protocols/{protocol_name}/contributors",
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
            "content": {
                "application/json": {"example": {"error": "Error description"}}
            },
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

        doc_base = (
            db.collection(f"{protocol_name}-widgets")
            .document("repositories")
            .collection(f"{owner}#{repo}")
        )

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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    # data = ref.get('data', None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


@app.get(
    "/protocols/{protocol_name}/community-profile",
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
            "content": {
                "application/json": {"example": {"error": "Error description"}}
            },
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


@app.get(
    "/protocols/{protocol_name}/language-breakdown",
    dependencies=[Depends(get_current_user)],
    tags=["Github - Project"],
    responses={
        200: {
            "description": "Language breakdown",
            "content": {
                "application/json": {
                    "example": [
                        {"name": "JavaScript", "percentage": 100, "size": 55852}
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
            "content": {
                "application/json": {"example": {"error": "Error description"}}
            },
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


@app.get(
    "/protocols/{protocol_name}/issue-count",
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
            "content": {
                "application/json": {"example": {"error": "Error description"}}
            },
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

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


@app.get(
    "/protocols/{protocol_name}/most-active-issues",
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
            "content": {
                "application/json": {"example": {"error": "Error description"}}
            },
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    interval_data = data.get(interval.value, [])
    if not interval_data:
        raise HTTPException(
            status_code=204, detail="Content is empty for the given interval."
        )

    return interval_data


@app.get(
    "/protocols/{protocol_name}/pull-request-count",
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
            "content": {
                "application/json": {"example": {"error": "Error description"}}
            },
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    if ref2:
        data["average_days_to_close_pull_requests"] = ref2.get("data", 0)

    return data


class RecentIssuesOrder(str, Enum):
    created_at = "created_at"
    updated_at = "updated_at"


@app.get(
    "/protocols/{protocol_name}/recent-issues",
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
            "content": {
                "application/json": {"example": {"error": "Error description"}}
            },
        },
    },
)
def recent_issues(
    protocol_name: str = Path(..., description="Protocol name"),
    owner: str = Query(..., description="Project owner name"),
    repo: str = Query(..., description="Project repository name"),
    order_by: RecentIssuesOrder = Query(
        RecentIssuesOrder.created_at, description="Order by field"
    ),
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
            raise HTTPException(
                status_code=404, detail="Collection or document not found."
            )

    except exceptions.NotFound as ex:
        # Handle case where document or collection does not exist
        raise HTTPException(status_code=404, detail=str(ex))

    except Exception as e:
        # Handle other exceptions
        raise HTTPException(
            status_code=500, detail=f"An error occurred: {str(e)}"
        )

    data = collection_ref.get("data", None)

    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


class RecentPullRequestOrder(str, Enum):
    created_at = "created_at"
    updated_at = "updated_at"


@app.get(
    "/protocols/{protocol_name}/recent-pull-requests",
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
            "content": {
                "application/json": {"example": {"error": "Error description"}}
            },
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = collection_ref.get("data", None)

    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


@app.get(
    "/protocols/{protocol_name}/recent-stargazing-activity",
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
            "content": {
                "application/json": {"example": {"error": "Error description"}}
            },
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if (not data) or (not data["series"][0]["data"]):
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


class IssueActivityInterval(str, Enum):
    week = "week"
    month = "month"
    year = "year"


@app.get(
    "/protocols/{protocol_name}/issue-activity",
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
            "content": {
                "application/json": {"example": {"error": "Error description"}}
            },
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

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


@app.get(
    "/protocols/{protocol_name}/pull-request-activity",
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
            "content": {
                "application/json": {"example": {"error": "Error description"}}
            },
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

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


@app.get(
    "/protocols/{protocol_name}/recent-commits",
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
            "content": {
                "application/json": {"example": {"error": "Error description"}}
            },
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


@app.get(
    "/protocols/{protocol_name}/recent-releases",
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
            "content": {
                "application/json": {"example": {"error": "Error description"}}
            },
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data[-10:]


@app.get(
    "/protocols/{protocol_name}/cumulative-stats",
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
def cumulative_stats(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the cumulative stats for the protocol.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-cumulative")
            .document(f"cumulative_info")
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


@app.get(
    "/protocols/{protocol_name}/cumulative-commit-activity",
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
def cumulative_commit_activity(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the cumulative commit activity for the protocol.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-cumulative")
            .document(f"cumulative_commit_activity")
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


@app.get(
    "/protocols/{protocol_name}/cumulative-participation",
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
def cumulative_participation(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the cumulative participation for the protocol.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-cumulative")
            .document(f"cumulative_participation")
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


@app.get(
    "/protocols/{protocol_name}/cumulative-code-frequency",
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
def cumulative_code_frequency(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the cumulative code frequency for the protocol.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-cumulative")
            .document(f"cumulative_code_frequency")
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


@app.get(
    "/protocols/{protocol_name}/cumulative-punch-card",
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
def cumulative_punch_card(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the cumulative punch card for the protocol.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-cumulative")
            .document(f"cumulative_punch_card")
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


@app.get(
    "/protocols/{protocol_name}/cumulative-language-breakdown",
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
def cumulative_language_breakdown(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the cumulative language breakdown for the protocol.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-cumulative")
            .document(f"cumulative_language_breakdown")
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


@app.get(
    "/protocols/{protocol_name}/cumulative-issue-count",
    dependencies=[Depends(get_current_user)],
    tags=["Github - Ecosystem"],
    responses={
        200: {
            "description": "Cumulative Issue Count",
            "content": {
                "application/json": {"example": {"closed": 12, "open": 2}}
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
def cumulative_issue_count(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the cumulative issue count for the protocol.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-cumulative")
            .document(f"cumulative_issue_count")
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


class CumulativeMostActiveIssuesInterval(str, Enum):
    day = "day"
    week = "week"
    month = "month"
    year = "year"


@app.get(
    "/protocols/{protocol_name}/cumulative-most-active-issues",
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
def cumulative_most_active_issues(
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
            .document(f"cumulative_most_active_issues")
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    interval_data = data.get(interval, [])
    if not interval_data:
        raise HTTPException(
            status_code=204, detail="No data for the given interval."
        )

    return interval_data


@app.get(
    "/protocols/{protocol_name}/cumulative-pull-request-count",
    dependencies=[Depends(get_current_user)],
    tags=["Github - Ecosystem"],
    responses={
        200: {
            "description": "Cumulative Pull Request Count",
            "content": {
                "application/json": {"example": {"closed": 12, "open": 2}}
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
def cumulative_pull_request_count(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the cumulative pull request count for the protocol.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-cumulative")
            .document(f"cumulative_pull_request_count")
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


class CumulativeRecentIssuesOrder(str, Enum):
    created_at = "created_at"
    updated_at = "updated_at"


@app.get(
    "/protocols/{protocol_name}/cumulative-recent-issues",
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
def cumulative_recent_issues(
    protocol_name: str = Path(..., description="Protocol name"),
    order_by: CumulativeRecentIssuesOrder = Query(
        ..., description="Order by field"
    ),
):
    """
    Returns the cumulative recent issues for the protocol.

    `order_by` can be one of the following values: created_at, updated_at

    """

    field_name = None
    if order_by == RecentIssuesOrder.created_at:
        field_name = "recent_created_issues"

    elif order_by == RecentIssuesOrder.updated_at:
        field_name = f"recent_updated_issues"

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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


@app.get(
    "/protocols/{protocol_name}/cumulative-recent-pull-requests",
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
def cumulative_recent_pull_requests(
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
        field_name = f"recent_updated_pull_requests"

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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


@app.get(
    "/protocols/{protocol_name}/cumulative-recent-commits",
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
def cumulative_recent_commits(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the cumulative recent commits for the protocol.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-cumulative")
            .document(f"cumulative_recent_commits")
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


@app.get(
    "/protocols/{protocol_name}/cumulative-recent-releases",
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
def cumulative_recent_releases(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the cumulative recent releases for the protocol.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-cumulative")
            .document(f"cumulative_recent_releases")
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


class TopicActivityInterval(str, Enum):
    yearly = "yearly"
    monthly = "monthly"
    weekly = "weekly"
    daily = "daily"


@app.get(
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
                        "series": [
                            {"data": [128, 535, 246, 100], "type": "line"}
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
def discourse_topic_activity(
    protocol_name: str = Path(..., description="Protocol name"),
    interval: TopicActivityInterval = Query(..., description="Interval"),
):
    """
    Returns the discourse topic activity for the protocol.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-discourse")
            .document(f"topic_activity")
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

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


@app.get(
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
            "content": {
                "application/json": {"example": {"error": "Error description"}}
            },
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
        ref = (
            db.collection(f"{protocol_name}-discourse")
            .document(f"topic_metrics")
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


@app.get(
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
            "content": {
                "application/json": {"example": {"error": "Error description"}}
            },
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
        ref = (
            db.collection(f"{protocol_name}-discourse")
            .document(f"user_metrics")
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


@app.get(
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
            "content": {
                "application/json": {"example": {"error": "Error description"}}
            },
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
        ref = (
            db.collection(f"{protocol_name}-discourse")
            .document(f"categories")
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


@app.get(
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
            "content": {
                "application/json": {"example": {"error": "Error description"}}
            },
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
        ref = (
            db.collection(f"{protocol_name}-discourse")
            .document(f"tags")
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

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


@app.get(
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
            "content": {
                "application/json": {"example": {"error": "Error description"}}
            },
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
        ref = (
            db.collection(f"{protocol_name}-discourse")
            .document(f"top_topics")
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

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


@app.get(
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
            "content": {
                "application/json": {"example": {"error": "Error description"}}
            },
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
        ref = (
            db.collection(f"{protocol_name}-discourse")
            .document(f"latest_topics")
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

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


@app.get(
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
            "content": {
                "application/json": {"example": {"error": "Error description"}}
            },
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
        ref = (
            db.collection(f"{protocol_name}-discourse")
            .document(f"latest_posts")
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

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


@app.get(
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
            "content": {
                "application/json": {"example": {"error": "Error description"}}
            },
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
        ref = (
            db.collection(f"{protocol_name}-discourse")
            .document(f"top_users")
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

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


@app.get(
    "/protocols/{protocol_name}/developers-full-time",
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
def developers_full_time(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the number of developers working full time on the protocol.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-developers")
            .document(f"full_time")
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


@app.get(
    "/protocols/{protocol_name}/developers-monthly-active-devs",
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
            .document(f"monthly_active_devs")
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


@app.get(
    "/protocols/{protocol_name}/developers-total-repos",
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
            .document(f"total_repos")
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


@app.get(
    "/protocols/{protocol_name}/developers-total-commits",
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
            .document(f"total_commits")
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


@app.get(
    "/protocols/{protocol_name}/developers-monthly-active-dev-chart",
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
                                "data": [
                                    {"date": "1423440000000", "value": 32}
                                ],
                            },
                            {
                                "name": "Part-Time Developers",
                                "data": [
                                    {"date": "1423440000000", "value": 15}
                                ],
                            },
                            {
                                "name": "One-Time Developers",
                                "data": [
                                    {"date": "1423440000000", "value": 23}
                                ],
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
            .document(f"monthly_active_dev_chart")
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


@app.get(
    "/protocols/{protocol_name}/developers-total-monthly-active-dev-chart",
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
                                "data": [
                                    {"date": "1423440000000", "value": 32}
                                ],
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
            .document(f"total_monthly_active_dev_chart")
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


@app.get(
    "/protocols/{protocol_name}/developers-dev-type-table",
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
            .document(f"dev_type_table")
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


@app.get(
    "/protocols/{protocol_name}/developers-monthly-commits-by-dev-type-chart",
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
                                "data": [
                                    {"date": "1423440000000", "value": 32}
                                ],
                            },
                            {
                                "name": "Part-Time Developers",
                                "data": [
                                    {"date": "1423440000000", "value": 15}
                                ],
                            },
                            {
                                "name": "One-Time Developers",
                                "data": [
                                    {"date": "1423440000000", "value": 23}
                                ],
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
            .document(f"monthly_commits_by_dev_type_chart")
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


@app.get(
    "/protocols/{protocol_name}/developers-monthly-commits-chart",
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
                                "data": [
                                    {"date": "1423440000000", "value": 32}
                                ],
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
            .document(f"monthly_commits_chart")
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


class VotingPowerInterval(Enum):
    WEEK = "WEEK"
    MONTH = "MONTH"
    YEAR = "YEAR"


@app.get(
    "/protocols/{protocol_name}/governance-voting-power-chart",
    tags=["Governance - Ecosystem"],
    dependencies=[Depends(get_current_user)],
    responses={
        200: {
            "description": "Voting Power Chart",
            "content": {
                "application/json": {
                    "example": {
                        "yAxis": {"type": "value"},
                        "xAxis": {"type": "time"},
                        "series": [
                            {
                                "twitter": "",
                                "address": "0xea6C3Db2e7FCA00Ea9d7211a03e83f568Fc13BF7",
                                "data": [
                                    {
                                        "balance": 331.07,
                                        "timestamp": "2023-01-01T00:00:00Z",
                                    }
                                ],
                                "name": "Polychain Capital",
                                "bio": "",
                                "tally_url": "https://www.tally.xyz/profile/0xea6C3Db2e7FCA00Ea9d7211a03e83f568Fc13BF7?governanceId=eip155:1:0xc0Da02939E1441F497fd74F78cE7Decb17B66529",
                                "ens": "",
                                "type": "line",
                                "picture": "https://static.tally.xyz/7b888910-fdfb-40af-84b1-09847c6054b2_400x400.jpg",
                                "email": "",
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
def governance_voting_power_chart(
    protocol_name: str = Path(..., description="Protocol name"),
    interval: VotingPowerInterval = Query(
        VotingPowerInterval.WEEK, description="Interval to group by"
    ),
):
    """
    Returns the voting power chart.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-governance")
            .document(f"voting_power_chart_{interval.value.lower()}")
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


class DelegateSortField(Enum):
    CREATED = "CREATED"
    UPDATED = "UPDATED"
    TOKENS_OWNED = "TOKENS_OWNED"
    VOTING_WEIGHT = "VOTING_WEIGHT"
    DELEGATIONS = "DELEGATIONS"
    HAS_ENS = "HAS_ENS"
    HAS_DELEGATE_STATEMENT = "HAS_DELEGATE_STATEMENT"
    PROPOSALS_CREATED = "PROPOSALS_CREATED"
    VOTES_CAST = "VOTES_CAST"


@app.get(
    "/protocols/{protocol_name}/governance-delegates",
    tags=["Governance - Ecosystem"],
    dependencies=[Depends(get_current_user)],
    responses={
        200: {
            "description": "Delegates",
            "content": {
                "application/json": {
                    "example": [
                        {
                            "participation": {
                                "stats": {
                                    "delegationCount": 162,
                                    "votingPower": {
                                        "in": 330.94,
                                        "net": 330.94,
                                        "out": 0,
                                    },
                                    "activeDelegationCount": 119,
                                    "weight": {"total": 330.94, "owned": 0},
                                    "votes": {"total": 47},
                                    "voteCount": 47,
                                    "delegations": {"total": 162},
                                    "recentParticipationRate": {
                                        "recentProposalCount": 10,
                                        "recentVoteCount": 8,
                                    },
                                    "tokenBalance": 0,
                                    "createdProposalsCount": 0,
                                }
                            },
                            "account": {
                                "twitter": "",
                                "address": "0xea6C3Db2e7FCA00Ea9d7211a03e83f568Fc13BF7",
                                "name": "Polychain Capital",
                                "bio": "",
                                "tally_url": "https://www.tally.xyz/profile/0xea6C3Db2e7FCA00Ea9d7211a03e83f568Fc13BF7?governanceId=eip155:1:0xc0Da02939E1441F497fd74F78cE7Decb17B66529",
                                "id": "eip155:1:0xea6C3Db2e7FCA00Ea9d7211a03e83f568Fc13BF7",
                                "ens": "",
                                "picture": "https://static.tally.xyz/7b888910-fdfb-40af-84b1-09847c6054b2_400x400.jpg",
                                "email": "",
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
            "content": {
                "application/json": {"example": {"error": "Error description"}}
            },
        },
    },
)
def governance_delegates(
    protocol_name: str = Path(..., description="Protocol name"),
    sort_by: DelegateSortField = Query(
        DelegateSortField.VOTING_WEIGHT, description="Sort by"
    ),
):
    """
    Returns the delegates sorted by the given field.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-governance")
            .document(f"delegates_{sort_by.value.lower()}")
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


@app.get(
    "/protocols/{protocol_name}/governance-proposals",
    tags=["Governance - Ecosystem"],
    dependencies=[Depends(get_current_user)],
    responses={
        200: {
            "description": "Governance Proposals",
            "content": {
                "application/json": {
                    "example": [
                        {
                            "proposer": {
                                "twitter": "",
                                "address": "0x7B3c54e17d618CC94daDFe7671c1e2F50C4Ecc33",
                                "name": "0x7B3c54e17d618CC94daDFe7671c1e2F50C4Ecc33",
                                "bio": "",
                                "tally_url": "https://www.tally.xyz/profile/0x7B3c54e17d618CC94daDFe7671c1e2F50C4Ecc33?governanceId=eip155:1:0xc0Da02939E1441F497fd74F78cE7Decb17B66529",
                                "id": "eip155:1:0x7B3c54e17d618CC94daDFe7671c1e2F50C4Ecc33",
                                "ens": "",
                                "picture": None,
                                "email": "",
                            },
                            "start": {"timestamp": "2023-07-26T18:56:47Z"},
                            "createdTransaction": {
                                "block": {
                                    "number": 17766046,
                                    "timestamp": "2023-07-24T22:48:59Z",
                                }
                            },
                            "description": "# [Gauntlet] 2023-07-24 Pause supply for Compound v2 Tail Assets...",
                            "statusChanges": [
                                {
                                    "type": "PENDING",
                                    "txHash": "0x410ebdda821c1b7011f519b22cb3a681443be6ba02da0ce2e5bc23c377996e84",
                                    "transaction": {
                                        "block": {
                                            "number": 17766046,
                                            "timestamp": "2023-07-24T22:48:59Z",
                                        }
                                    },
                                },
                                {
                                    "type": "ACTIVE",
                                    "txHash": "",
                                    "transaction": None,
                                },
                                {
                                    "type": "SUCCEEDED",
                                    "txHash": "",
                                    "transaction": None,
                                },
                                {
                                    "type": "QUEUED",
                                    "txHash": "0x5b3de1ae78d5eb18f7001a361f6b6ede8cde50b27ea5a1dabd48b42e3519a6b3",
                                    "transaction": {
                                        "block": {
                                            "number": 17798899,
                                            "timestamp": "2023-07-29T13:09:35Z",
                                        }
                                    },
                                },
                                {
                                    "type": "EXECUTED",
                                    "txHash": "0xf6ae0641f32b2ad24ec576fe71a1cd0ffb122f00b8cd46ae4741830ceae37bfe",
                                    "transaction": {
                                        "block": {
                                            "number": 17813201,
                                            "timestamp": "2023-07-31T13:10:35Z",
                                        }
                                    },
                                },
                            ],
                            "tally_url": "https://www.tally.xyz/gov/compound/proposal/170",
                            "title": "# [Gauntlet] 2023-07-24 Pause supply for Compound v2 Tail Assets",
                            "executable": {
                                "values": ["0", "0", "0", "0", "0"],
                                "callDatas": [
                                    "0x000000000000000000000000e65cdb6479bac1e22340e4e755fae7e509ecd06c0000000000000000000000000000000000000000000000000000000000000001",
                                    "0x00000000000000000000000070e36f6bf80a52b3b46b3af8e106cc0ed743e8e40000000000000000000000000000000000000000000000000000000000000001",
                                    "0x000000000000000000000000face851a4921ce59e912d19329929ce6da6eb0c70000000000000000000000000000000000000000000000000000000000000001",
                                    "0x0000000000000000000000004b0181102a0112a2ef11abee5563bb4a3176c9d70000000000000000000000000000000000000000000000000000000000000001",
                                    "0x00000000000000000000000035a18000230da775cac24873d00ff85bccded5500000000000000000000000000000000000000000000000000000000000000001",
                                ],
                                "targets": [
                                    "0x3d9819210A31b4961b30EF54bE2aeD79B9c9Cd3B",
                                    "0x3d9819210A31b4961b30EF54bE2aeD79B9c9Cd3B",
                                    "0x3d9819210A31b4961b30EF54bE2aeD79B9c9Cd3B",
                                    "0x3d9819210A31b4961b30EF54bE2aeD79B9c9Cd3B",
                                    "0x3d9819210A31b4961b30EF54bE2aeD79B9c9Cd3B",
                                ],
                                "signatures": [
                                    "_setMintPaused(address,bool)",
                                    "_setMintPaused(address,bool)",
                                    "_setMintPaused(address,bool)",
                                    "_setMintPaused(address,bool)",
                                    "_setMintPaused(address,bool)",
                                ],
                            },
                            "eta": "1690808975",
                            "voteStats": [
                                {
                                    "weight": 810.03,
                                    "votes": "29",
                                    "support": "FOR",
                                    "percent": 100,
                                },
                                {
                                    "weight": 0,
                                    "votes": "0",
                                    "support": "AGAINST",
                                    "percent": 0,
                                },
                                {
                                    "weight": 0,
                                    "votes": "0",
                                    "support": "ABSTAIN",
                                    "percent": 0,
                                },
                            ],
                            "end": {"timestamp": "2023-07-29T13:08:59Z"},
                            "block": {
                                "number": 17766046,
                                "timestamp": "2023-07-24T22:48:59Z",
                            },
                            "votes": [
                                {
                                    "reason": "",
                                    "weight": 50,
                                    "tally_url": "https://www.tally.xyz/profile/0xed11e5eA95a5A3440fbAadc4CC404C56D0a5bb04?governanceId=eip155:1:0xc0Da02939E1441F497fd74F78cE7Decb17B66529",
                                    "voter": {
                                        "twitter": "she256.eth",
                                        "address": "0xed11e5eA95a5A3440fbAadc4CC404C56D0a5bb04",
                                        "name": "she256.eth",
                                        "bio": "",
                                        "id": "eip155:1:0xed11e5eA95a5A3440fbAadc4CC404C56D0a5bb04",
                                        "ens": "she256.eth",
                                        "picture": None,
                                        "email": "",
                                    },
                                    "support": "FOR",
                                    "transaction": {
                                        "block": {
                                            "number": 17792597,
                                            "timestamp": "2023-07-28T15:59:11Z",
                                        }
                                    },
                                }
                            ],
                            "governor": {"quorum": "400000000000000000000000"},
                            "id": 170,
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
def governance_proposals(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the governance proposals.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-governance")
            .document(f"proposals")
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


@app.get(
    "/protocols/{protocol_name}/governance-info",
    tags=["Governance - Ecosystem"],
    dependencies=[Depends(get_current_user)],
    responses={
        200: {
            "description": "Governance Info",
            "content": {
                "application/json": {
                    "example": {
                        "chainId": "eip155:1",
                        "stats": {
                            "tokens": {
                                "delegatedVotingPower": 2112.06,
                                "voters": 4911,
                                "owners": 213260,
                                "delegates": {"total": 4911},
                                "supply": 10000,
                            },
                            "proposals": {
                                "total": 128,
                                "active": 0,
                                "failed": 32,
                                "passed": 96,
                            },
                        },
                        "kind": "SINGLE_GOV",
                        "organization": {
                            "website": "https://compound.finance/",
                            "name": "Compound",
                            "description": "Compound is an algorithmic, autonomous interest rate protocol built for developers, to unlock a universe of open financial applications.",
                            "visual": {
                                "color": "#00d395",
                                "icon": "https://static.withtally.com/bc952927-da93-4cab-b0ce-b5e2f5976b9a_400x400.jpg",
                            },
                            "tally_url": "https://www.tally.xyz/gov/compound",
                            "id": "1",
                            "slug": "compound",
                            "votingParameters": {
                                "bigVotingPeriod": "19710",
                                "quorum": 400,
                                "votingPeriod": 0,
                            },
                        },
                        "timelockId": "eip155:1:0x6d903f6003cca6255D85CcA4D3B5E5146dC33925",
                        "active": True,
                        "tokens": [
                            {
                                "symbol": "COMP",
                                "lastIndexedBlock": {
                                    "number": 17835154,
                                    "timestamp": "2023-08-03T14:46:47Z",
                                },
                                "isIndexing": True,
                                "decimals": 18,
                                "name": "Compound",
                                "id": "eip155:1/erc20:0xc00e94Cb662C3520282E6f5717214004A7f26888",
                                "type": "ERC20",
                            }
                        ],
                        "id": "eip155:1:0xc0Da02939E1441F497fd74F78cE7Decb17B66529",
                        "contracts": {
                            "timelock": {
                                "address": "0x6d903f6003cca6255D85CcA4D3B5E5146dC33925"
                            },
                            "tokens": [
                                {
                                    "address": "0xc00e94Cb662C3520282E6f5717214004A7f26888"
                                }
                            ],
                            "governor": {
                                "address": "0xc0Da02939E1441F497fd74F78cE7Decb17B66529"
                            },
                        },
                        "proposalThreshold": 25,
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
def governance_info(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the governance info.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-governance")
            .document(f"governance_info")
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


@app.get(
    "/protocols/{protocol_name}/governance-safes",
    tags=["Governance - Ecosystem"],
    dependencies=[Depends(get_current_user)],
    responses={
        200: {
            "description": "Governance Safes",
            "content": {
                "application/json": {
                    "example": {
                        "gnosisSafes": [
                            {
                                "balance": {
                                    "totalUSDValue": "264059.3134",
                                    "tokens": [
                                        {
                                            "symbol": "COMP",
                                            "amount": "4370761897000000000000",
                                            "address": "0xc00e94Cb662C3520282E6f5717214004A7f26888",
                                            "decimals": 18,
                                            "name": "Compound",
                                            "fiat": "259415.0321",
                                            "logoURI": "https://safe-transaction-assets.safe.global/tokens/logos/0xc00e94Cb662C3520282E6f5717214004A7f26888.png",
                                        },
                                    ],
                                },
                                "name": "Compound Grants Program",
                                "threshold": 3,
                                "owners": [
                                    {
                                        "address": "0x66cD62c6F8A4BB0Cd8720488BCBd1A6221B765F9",
                                        "name": "allthecolors",
                                        "bio": "Here to help, I hope. Euler delegate; Compound governance participant; ad hoc contributor on dev and analytics for Compound and Aztec Connect; ardent non-maximalist",
                                        "tally_url": "https://www.tally.xyz/profile/0x66cD62c6F8A4BB0Cd8720488BCBd1A6221B765F9?governanceId=eip155:1:0xc0Da02939E1441F497fd74F78cE7Decb17B66529",
                                        "id": "eip155:1:0x66cD62c6F8A4BB0Cd8720488BCBd1A6221B765F9",
                                        "picture": "https://static.tally.xyz/10b676fb-6d18-45f8-ae15-6160b66bfc59_400x400.jpg",
                                    },
                                ],
                                "tally_url": "https://www.tally.xyz/safe/eip155:1:0x8524B12CB7710C75B53bAa9ca72B420542d24C13",
                                "id": "eip155:1:0x8524B12CB7710C75B53bAa9ca72B420542d24C13",
                                "nonce": 52,
                                "version": "1.3.0",
                            }
                        ],
                        "latestUpdate": "2023-08-03T14:40:04.760Z",
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
def governance_safes(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the governance Gnosis safes.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-governance")
            .document(f"safes")
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


@app.get(
    "/protocols/{protocol_name}/messari-asset",
    tags=["Messari - Ecosystem"],
    dependencies=[Depends(get_current_user)],
    responses={
        200: {
            "description": "Asset Info",
            "content": {
                "application/json": {
                    "example": {
                        "data": {
                            "symbol": "COMP",
                            "_internal_temp_agora_id": "b4a4024c-14c1-4e90-b092-b27cb58b7521",
                            "serial_id": 350,
                            "name": "Compound",
                            "id": "157f4fe3-6046-4b6d-bceb-a2af8ca021b5",
                            "contract_addresses": [
                                {
                                    "contract_address": "0x8505b9d2254a7ae468c0e9dd10ccea3a837aef5c",
                                    "platform": "polygon-pos",
                                },
                                {
                                    "contract_address": "0x354a6da3fcde098f8389cad84b0182725c6c91de",
                                    "platform": "arbitrum-one",
                                },
                                {
                                    "contract_address": "c00e94cb662c3520282e6f5717214004a7f26888.factory.bridge.near",
                                    "platform": "near-protocol",
                                },
                                {
                                    "contract_address": "0x32137b9275ea35162812883582623cd6f6950958",
                                    "platform": "harmony-shard-0",
                                },
                                {
                                    "contract_address": "0x66bc411714e16b6f0c68be12bd9c666cc4576063",
                                    "platform": "energi",
                                },
                                {
                                    "contract_address": "0x52ce071bd9b1c4b00a0b92d298c512478cad67e8",
                                    "platform": "binance-smart-chain",
                                },
                                {
                                    "contract_address": "0xc3048e19e76cb9a3aa9d77d8c03c29fc906e2437",
                                    "platform": "avalanche",
                                },
                                {
                                    "contract_address": "0xc00e94cb662c3520282e6f5717214004a7f26888",
                                    "platform": "ethereum",
                                },
                                {
                                    "contract_address": "0x00dbd45af9f2ea406746f9025110297469e9d29efc60df8d88efb9b0179d6c2c",
                                    "platform": "sora",
                                },
                            ],
                            "slug": "compound",
                        },
                        "status": {
                            "elapsed": 1,
                            "timestamp": "2023-08-03T18:34:21.65422534Z",
                        },
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
def messari_asset(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the asset data from Messari.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-messari")
            .document(f"asset")
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


@app.get(
    "/protocols/{protocol_name}/messari-asset-profile",
    tags=["Messari - Ecosystem"],
    #  dependencies=[Depends(get_current_user)],
    responses={
        200: {
            "description": "Messari Asset Profile",
            "content": {
                "application/json": {
                    "example": {
                        "data": {
                            "symbol": "COMP",
                            "_internal_temp_agora_id": "b4a4024c-14c1-4e90-b092-b27cb58b7521",
                            "profile": {
                                "general": {
                                    "overview": {
                                        "project_details": "Compound is a lending platform built on Ethereum that enables users to permissionlessly borrow or lend from a pool of assets. Rather than interest rates being set by individuals, they are determined algorithmically based on the proportion of assets lent out.",
                                        "tagline": "Decentralized money market protocol",
                                        "official_links": [
                                            {
                                                "name": "Website",
                                                "link": "https://compound.finance/",
                                            }
                                        ],
                                        "category": "Financial",
                                        "is_verified": False,
                                        "sector": "Lending",
                                        "tags": "DeFi",
                                    },
                                    "regulation": {
                                        "sfar_summary": None,
                                        "regulatory_details": None,
                                        "sfar_score": None,
                                    },
                                    "background": {
                                        "background_details": "Compound was started ...",
                                        "issuing_organizations": [],
                                    },
                                    "roadmap": [
                                        {
                                            "date": "2018-09-27T04:00:00Z",
                                            "details": None,
                                            "title": "Compound launches on mainnet",
                                            "type": None,
                                        }
                                    ],
                                },
                                "economics": {
                                    "consensus_and_emission": {
                                        "consensus": {
                                            "precise_consensus_mechanism": None,
                                            "is_victim_of_51_percent_attack": None,
                                            "block_reward": None,
                                            "consensus_details": "",
                                            "next_halving_date": None,
                                            "general_consensus_mechanism": "n/a",
                                            "targeted_block_time": None,
                                            "mining_algorithm": None,
                                        },
                                        "supply": {
                                            "max_supply": 10000000,
                                            "is_capped_supply": True,
                                            "precise_emission_type": "Fixed Supply",
                                            "supply_curve_details": "A total of 10 million COMP tokens ...",
                                            "general_emission_type": "Fixed Supply",
                                        },
                                    },
                                    "native_treasury": {
                                        "treasury_usage_details": None,
                                        "accounts": [],
                                    },
                                    "launch": {
                                        "general": {
                                            "launch_style": "Private Sale",
                                            "launch_details": "Tokens were initially distributed to shareholders of Compound Labs, Inc. who trialed token governance for a few months. Afterward, the distribution for users of the platform began, releasing tokens continuously for the next four years split equally between lenders and borrowers of various pools based on the proportion of total interest paid out. This process, known as liquidity mining, generated substantial attention around the DeFi community and increased the total value of assets locked from $100 million to over $600 million in the first week.",
                                        },
                                        "fundraising": {
                                            "sales_documents": [
                                                {
                                                    "name": "SEC Form D ",
                                                    "link": "https://www.sec.gov/Archives/edgar/data/1736957/000173695718000001/xslFormDX01/primary_doc.xml",
                                                }
                                            ],
                                            "sales_treasury_accounts": [],
                                            "projected_use_of_sales_proceeds": [],
                                            "sales_rounds": [
                                                {
                                                    "end_date": None,
                                                    "asset_collected": None,
                                                    "amount_collected_in_asset": None,
                                                    "is_kyc_required": None,
                                                    "title": "Seed Equity",
                                                    "type": "Private",
                                                    "native_tokens_allocated": None,
                                                    "price_per_token_in_asset": None,
                                                    "equivalent_price_per_token_in_usd": None,
                                                    "amount_collected_in_usd": 8200000,
                                                    "restricted_jurisdictions": None,
                                                    "details": None,
                                                    "start_date": None,
                                                },
                                            ],
                                            "treasury_policies": None,
                                        },
                                        "initial_distribution": {
                                            "genesis_block_date": None,
                                            "initial_supply_repartition": {
                                                "allocated_to_organization_or_founders_percentage": None,
                                                "allocated_to_investors_percentage": None,
                                                "allocated_to_premined_rewards_or_airdrops_percentage": None,
                                            },
                                            "initial_supply": 10000000,
                                            "token_distribution_date": "2020-04-17T05:00:00Z",
                                        },
                                    },
                                    "token": {
                                        "multitoken": [],
                                        "token_usage": "Vote",
                                        "token_name": "Compound governance token",
                                        "token_address": "0xc00e94cb662c3520282e6f5717214004a7f26888",
                                        "token_type": "ERC-20",
                                        "token_usage_details": "The COMP token is used ...",
                                        "block_explorers": None,
                                    },
                                },
                                "metadata": {
                                    "updated_at": "2021-07-24T08:23:17Z"
                                },
                                "ecosystem": {
                                    "assets": [
                                        {
                                            "name": "Maker",
                                            "id": "6d06be66-d8c6-4e24-adfb-779b1afc3c9a",
                                        }
                                    ],
                                    "organizations": [],
                                },
                                "advisors": {
                                    "organizations": [],
                                    "individuals": [],
                                },
                                "contributors": {
                                    "organizations": [
                                        {
                                            "name": "Compound",
                                            "logo": "https://messari.s3.amazonaws.com/images/agora-images/0%3Fe%3D1559174400%26v%3Dbeta%26t%3DIzRX1KSfPGlT_Q0nXZTpYgFhg9GkYJEMpUAqKLCQN9M",
                                            "description": "Compound is a decentralized protocol ...",
                                            "slug": "compound",
                                        }
                                    ],
                                    "individuals": [
                                        {
                                            "avatar_url": "https://messari.s3.amazonaws.com/images/agora-images/0%3Fe%3D1554336000%26v%3Dbeta%26t%3DYwyHmqgg_RHYCrnb9nioPtfAnVHJB3p9CgXb-vqY5tc",
                                            "last_name": "Hayes",
                                            "description": None,
                                            "title": "CTO of Compound",
                                            "first_name": "Geoffrey",
                                            "slug": "geoffrey-hayes",
                                        },
                                    ],
                                },
                                "technology": {
                                    "overview": {
                                        "client_repositories": [],
                                        "technology_details": "Compound pools are created by users supplying assets in...",
                                    },
                                    "security": {
                                        "known_exploits_and_vulnerabilities": [
                                            {
                                                "date": "2018-12-09T05:00:00Z",
                                                "details": "An outside party discovered a defect...",
                                                "title": "Potential exploit",
                                                "type": "Bug disclosure",
                                            }
                                        ],
                                        "audits": [
                                            {
                                                "date": "2019-04-08T04:00:00Z",
                                                "details": "Trail of Bits [audited](https://github.com/trailofbits/publications/blob/master/reviews/compound-2.pdf) Compound's smart contracts ... ",
                                                "title": "Compound v2 Security Assessment",
                                                "type": None,
                                            }
                                        ],
                                    },
                                },
                                "investors": {
                                    "organizations": [
                                        {
                                            "name": "Coinbase Ventures",
                                            "logo": "https://messari.s3.amazonaws.com/images/agora-images/HVEeWmV4_400x400.png",
                                            "description": "Coinbase Ventures invests in ...",
                                            "slug": "coinbase-ventures",
                                        }
                                    ],
                                    "individuals": [],
                                },
                                "governance": {
                                    "grants": [],
                                    "onchain_governance": {
                                        "is_treasury_decentralized": None,
                                        "onchain_governance_details": "Anyone with 1% of tokens held or delegated to ...",
                                        "onchain_governance_type": "Direct On-Chain Vote, Delegated On-Chain Vote",
                                    },
                                    "governance_details": "Compound is governed entirely on-chain by COMP holders where ...",
                                },
                            },
                            "serial_id": 350,
                            "name": "Compound",
                            "id": "157f4fe3-6046-4b6d-bceb-a2af8ca021b5",
                            "contract_addresses": [
                                {
                                    "contract_address": "0x52ce071bd9b1c4b00a0b92d298c512478cad67e8",
                                    "platform": "binance-smart-chain",
                                }
                            ],
                            "slug": "compound",
                        },
                        "status": {
                            "elapsed": 2,
                            "timestamp": "2023-08-03T18:34:22.085942893Z",
                        },
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
def messari_asset_profile(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the asset profile data from Messari.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-messari")
            .document(f"asset_profile")
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


@app.get(
    "/protocols/{protocol_name}/messari-metrics",
    tags=["Messari - Ecosystem"],
    #  dependencies=[Depends(get_current_user)],
    responses={
        200: {
            "description": "Messari metrics data",
            "content": {
                "application/json": {
                    "example": {
                        "data": {
                            "marketcap": {
                                "outstanding_marketcap_usd": 620156823.022705,
                                "liquid_marketcap_usd": 519758935.14514136,
                                "current_marketcap_usd": 409839830.6858402,
                                "marketcap_dominance_percent": 0.03409938740189166,
                                "realized_marketcap_usd": 830935523.8075892,
                                "rank": 100,
                                "y_2050_marketcap_usd": 597775239.1607776,
                                "volume_turnover_last_24_hours_percent": 13.20424173543578,
                                "y_plus10_marketcap_usd": 597775239.1607776,
                            },
                            "symbol": "COMP",
                            "supply_activity": {
                                "outstanding": None,
                                "supply_revived_2y": None,
                                "supply_revived_30d": None,
                                "supply_active_10y": 10000000,
                                "supply_revived_3y": None,
                                "supply_revived_4y": None,
                                "supply_revived_5y": None,
                                "supply_active_1y_percent": 78.713158897,
                                "supply_active_5y": 10000000,
                                "supply_active_30d": 5599983.319682396,
                                "supply_active_4y": 10000000,
                                "supply_active_3y": 9354878.551488778,
                                "supply_revived_1y": None,
                                "supply_revived_7d": None,
                                "supply_revived_90d": None,
                                "supply_active_90d": 6684090.390193483,
                                "supply_active_7d": 4029237.403186421,
                                "supply_active_ever": 10000000,
                                "supply_active_2y": 8741839.233954992,
                                "supply_active_1y": 7871315.889700713,
                                "supply_active_180d": 7136970.088110352,
                                "supply_active_1d": 3739927.788576772,
                            },
                            "all_time_high": {
                                "percent_down": -93.33990723306779,
                                "at": "2021-05-12T01:00:00Z",
                                "price": 905.1797142265216,
                                "days_since": 813,
                                "breakeven_multiple": 15.014805874252428,
                            },
                            "roi_data": {
                                "percent_change_last_1_week": -14.111830626725808,
                                "percent_change_btc_last_1_month": 1.4664804063154413,
                                "percent_change_last_1_month": -3.6079481532496995,
                                "percent_change_year_to_date": 91.49725474120488,
                                "percent_change_btc_last_1_week": -14.15603776451461,
                                "percent_change_last_1_year": 6.567877277512545,
                                "percent_change_btc_last_1_year": -16.762136970822635,
                                "percent_change_eth_last_1_week": -13.520045616560353,
                                "percent_change_month_to_date": -8.434394170372668,
                                "percent_change_btc_last_3_months": 49.683707801640935,
                                "percent_change_eth_last_3_months": 59.93476556318819,
                                "percent_change_eth_last_1_year": -6.65378736574656,
                                "percent_change_last_3_months": 48.11923792167192,
                                "percent_change_eth_last_1_month": 0.9753543275021778,
                                "percent_change_quarter_to_date": 14.963574978971039,
                            },
                            "supply": {
                                "circulating": 6856085.76329154,
                                "y_2050_issued_percent": 86.94889,
                                "y_2050": 10000000,
                                "liquid": 8694889,
                                "stock_to_flow": 6.662183523087308,
                                "y_plus10_issued_percent": 86.94889,
                                "y_plus10": 10000000,
                                "annual_inflation_percent": 15.010093860887702,
                                "supply_revived_90d": 10000000,
                            },
                            "cycle_low": {
                                "at": "2023-06-10T16:00:00Z",
                                "price": 25.827885552798527,
                                "percent_up": 133.41364337277759,
                                "days_since": 54,
                            },
                            "alert_messages": None,
                            "roi_by_year": {
                                "2021_usd_percent": 37.157090154113234,
                                "2012_usd_percent": 0,
                                "2018_usd_percent": 0,
                                "2011_usd_percent": 0,
                                "2015_usd_percent": 0,
                                "2019_usd_percent": 0,
                                "2014_usd_percent": 0,
                                "2016_usd_percent": 0,
                                "2020_usd_percent": None,
                                "2013_usd_percent": 0,
                                "2017_usd_percent": 0,
                            },
                            "token_sale_stats": {
                                "sale_proceeds_usd": None,
                                "sale_end_date": None,
                                "roi_since_sale_btc_percent": None,
                                "roi_since_sale_usd_percent": None,
                                "roi_since_sale_eth_percent": None,
                                "sale_start_date": None,
                            },
                            "serial_id": 350,
                            "reddit": {
                                "active_user_count": None,
                                "subscribers": None,
                            },
                            "misc_data": {
                                "asset_created_at": None,
                                "vladimir_club_cost": None,
                                "sectors": ["Lending"],
                                "btc_y2050_normalized_supply_price_usd": None,
                                "private_market_price_usd": None,
                                "asset_age_days": None,
                                "btc_current_normalized_supply_price_usd": None,
                                "categories": ["Financial"],
                                "tags": ["DeFi"],
                            },
                            "risk_metrics": {
                                "volatility_stats": {
                                    "volatility_last_1_year": 1.0479291787678886,
                                    "volatility_last_90_days": 1.3353319113065794,
                                    "volatility_last_30_days": 1.145703683617018,
                                    "volatility_last_3_years": 1.2420971735487798,
                                },
                                "sharpe_ratios": {
                                    "last_1_year": 0.5220986301173018,
                                    "last_90_days": 1.8101719209507514,
                                    "last_30_days": -0.05646285403953599,
                                    "last_3_years": 0.38147803461305513,
                                },
                            },
                            "id": "157f4fe3-6046-4b6d-bceb-a2af8ca021b5",
                            "on_chain_data": {
                                "txn_erc20_count_last_24_hours": None,
                                "hash_rate": None,
                                "txn_erc721_count_last_24_hours": None,
                                "realized_marketcap_usd": 830935523.8075892,
                                "adjusted_rvt": 21.851188377407,
                                "average_transfer_value_usd": 42576.74885282842,
                                "addresses_balance_greater_10_usd_count": 57914,
                                "block_height": None,
                                "adjusted_nvt_90d_moving_average": 50.60007862806452,
                                "adjusted_nvt": 16.308321374092,
                                "average_fee_native_units": None,
                                "average_transfer_value_native_units": 686.548099967766,
                                "transfer_count_last_24_hours": 1031,
                                "txn_contracts_count_last_24_hours": 394,
                                "addresses_balance_greater_1m_usd_count": 94,
                                "value_weighted_average_utxo_age": None,
                                "addresses_balance_greater_1_native_units_count": 31930,
                                "utxo_in_profit_count": None,
                                "average_txn_gas_used": None,
                                "issuance_rate": None,
                                "addresses_balance_greater_0_01_native_units_count": 158145,
                                "average_block_interval": None,
                                "addresses_balance_greater_10k_usd_count": 741,
                                "addresses_balance_greater_10m_usd_count": 11,
                                "txn_per_second_count": 0.010150462963,
                                "addresses_balance_greater_10k_native_units_count": 112,
                                "addresses_balance_greater_100_usd_count": 24567,
                                "new_issuance": None,
                                "txn_token_count_last_24_hours": None,
                                "average_block_weight": None,
                                "txn_gas_limit_last_24_hours": None,
                                "addresses_balance_greater_10_native_units_count": 6367,
                                "average_block_gas_limit": None,
                                "transfer_erc721_count_last_24_hours": None,
                                "txn_contracts_calls_count_last_24_hours": None,
                                "txn_count_last_24_hours": 877,
                                "issuance_total_usd": 0,
                                "uncle_rewards_last_24_hours_usd": None,
                                "txn_contracts_destruction_count_last_24_hours": None,
                                "uncle_rewards_last_24_hours_native_units": None,
                                "total_fees_last_24_hours": None,
                                "adjusted_txn_volume_last_24_hours_native_units": 613183.8937075749,
                                "active_addresses_received_count": 382,
                                "addresses_balance_greater_1_usd_count": 140555,
                                "median_fee_native_units": None,
                                "addresses_count": 213325,
                                "block_weight": None,
                                "txn_gas_used_last_24_hours": None,
                                "txn_contracts_calls_success_count_last_24_hours": None,
                                "utxo_in_loss_count": None,
                                "average_txn_gas_limit": None,
                                "average_utxo_age": None,
                                "median_transfer_value_native_units": 71.38721459145525,
                                "addresses_balance_greater_1k_usd_count": 4336,
                                "addresses_balance_greater_0_1_native_units_count": 68914,
                                "adjusted_rvt_90d_moving_average": 67.79801701541005,
                                "txn_contracts_creation_count_last_24_hours": None,
                                "active_addresses_sent_count": 319,
                                "issuance_rate_daily": None,
                                "addresses_balance_greater_0_001_native_units_count": 188068,
                                "addresses_balance_greater_100k_usd_count": 228,
                                "uncle_blocks_count_last_24_hours": None,
                                "median_transfer_value_usd": 4427.126820547698,
                                "average_fee_usd": None,
                                "issuance_total_native_units": 0,
                                "issuance_last_24_hours_native_units": None,
                                "transfer_erc_20_count_last_24_hours": None,
                                "adjusted_txn_volume_last_24_hours_usd": 38027017.54503817,
                                "active_addresses": 479,
                                "utxo_count_last_24_hours": None,
                                "median_utxo_age": None,
                                "blocks_added_last_24_hours": None,
                                "median_fee_usd": None,
                                "addresses_balance_greater_100_native_units_count": 1066,
                                "mining_revenue_from_uncle_blocks_percent_last_24_hours": None,
                                "txn_volume_last_24_hours_native_units": 707831.0910667668,
                                "addresses_balance_greater_1m_native_units_count": 1,
                                "block_size_bytes_total": None,
                                "addresses_balance_greater_100k_native_units_count": 14,
                                "addresses_balance_greater_1k_native_units_count": 279,
                                "block_size_bytes_average": None,
                                "total_fees_last_24_hours_usd": None,
                                "txn_volume_last_24_hours_usd": 43896628.06726611,
                            },
                            "slug": "compound",
                            "developer_activity": {
                                "lines_added_last_1_year": None,
                                "commits_last_1_year": None,
                                "lines_added_last_3_months": None,
                                "lines_deleted_last_1_year": None,
                                "watchers": None,
                                "lines_deleted_last_3_months": None,
                                "stars": None,
                                "commits_last_3_months": None,
                            },
                            "_internal_temp_agora_id": "b4a4024c-14c1-4e90-b092-b27cb58b7521",
                            "mining_stats": {
                                "hash_rate": None,
                                "attack_appeal": None,
                                "mining_revenue_from_fees_percent_last_24_hours": None,
                                "network_hash_rate": None,
                                "mining_revenue_total": None,
                                "hash_rate_30d_average": None,
                                "available_on_nicehash_percent": None,
                                "average_difficulty": None,
                                "24_hours_attack_cost": None,
                                "mining_revenue_per_hash_native_units": None,
                                "mining_revenue_per_hash_per_second_native_units": None,
                                "mining_algo": None,
                                "mining_revenue_per_hash_per_second_usd": None,
                                "mining_revenue_per_hash_usd": None,
                                "mining_revenue_usd": None,
                                "1_hour_attack_cost": None,
                                "mining_revenue_native": None,
                            },
                            "market_data": {
                                "price_usd": 59.777523916077755,
                                "percent_change_btc_last_1_hour": 0.038696074800665836,
                                "percent_change_eth_last_24_hours": -5.298735742484862,
                                "percent_change_usd_last_1_hour": -0.4000698837566756,
                                "percent_change_btc_last_24_hours": -4.961118642141951,
                                "price_btc": 0.0020503463110269037,
                                "percent_change_usd_last_24_hours": -4.883462903234547,
                                "volume_last_24_hours_overstatement_multiple": 0.9987183556965813,
                                "last_trade_at": "2023-08-03T18:22:00Z",
                                "price_eth": 0.03247828838607769,
                                "real_volume_last_24_hours": 68630226.23809133,
                                "ohlcv_last_24_hour": {
                                    "volume": 68630226.23809133,
                                    "high": 63.893809223186324,
                                    "low": 58.69218657254978,
                                    "close": 59.777523916077755,
                                    "open": 63.0053017836613,
                                },
                                "percent_change_eth_last_1_hour": 0.21899233995534217,
                                "ohlcv_last_1_hour": {
                                    "volume": 807210.6209049412,
                                    "high": 60.03510581968828,
                                    "low": 59.71934145210464,
                                    "close": 59.777523916077755,
                                    "open": 59.94327212848452,
                                },
                                "volume_last_24_hours": 68542266.69959095,
                            },
                            "supply_distribution": {
                                "supply_in_addresses_balance_greater_10k_native_units": 8989878.05105035,
                                "supply_in_top_10_percent_addresses": 9968213.353461752,
                                "supply_in_addresses_balance_greater_0_01_native_units": 9999883.87804697,
                                "supply_in_addresses_balance_greater_1_native_units": 9983168.586671175,
                                "supply_in_addresses_balance_greater_100_native_units": 9756674.46800815,
                                "supply_in_top_1_percent_addresses": 9822040.606222052,
                                "supply_in_addresses_balance_greater_10k_usd": 9716556.529407993,
                                "supply_in_addresses_balance_greater_1m_usd": 8772281.454368787,
                                "supply_in_addresses_balance_greater_100_usd": 9974206.797289645,
                                "supply_in_addresses_balance_greater_100k_usd": 9484507.892288864,
                                "supply_in_utxo_in_loss": None,
                                "supply_in_addresses_balance_greater_10m_usd": 4975330.498556008,
                                "supply_in_addresses_balance_greater_1_usd": 9999660.801402632,
                                "supply_in_addresses_balance_greater_0_1_native_units": 9996350.798762387,
                                "supply_in_contracts_native_units": 3554718.3765288335,
                                "supply_in_addresses_balance_greater_0_001_native_units": 9999991.651076272,
                                "supply_in_contracts_usd": 220448285.5128549,
                                "supply_in_addresses_balance_greater_10_native_units": 9901732.770603864,
                                "supply_in_utxo_in_profit": None,
                                "supply_in_top_100_addresses": 8858464.983067088,
                                "supply_shielded": None,
                                "supply_in_addresses_balance_greater_1k_native_units": 9550087.80517032,
                                "supply_in_addresses_balance_greater_100k_native_units": 5314450.344210118,
                                "supply_in_addresses_balance_greater_1k_usd": 9876386.902556144,
                                "supply_in_addresses_balance_greater_1m_native_units": 1407057.5420756098,
                                "supply_in_addresses_balance_greater_10_usd": 9994941.288516559,
                            },
                            "exchange_flows": {
                                "flow_out_gemini_usd": None,
                                "flow_out_bitfinex_native_units": None,
                                "supply_huobi_native_units": None,
                                "flow_in_bitstamp_native_units": None,
                                "flow_net_bitmex_usd": None,
                                "flow_out_exchange_usd_inclusive": None,
                                "supply_bitfinex_usd": None,
                                "flow_net_bittrex_native_units": None,
                                "flow_out_poloniex_usd": None,
                                "flow_in_exchange_native_units": None,
                                "flow_net_bitmex_native_units": None,
                                "flow_in_poloniex_native_units": None,
                                "flow_out_poloniex_native_units": None,
                                "flow_net_binance_native_units": None,
                                "flow_in_kraken_usd": None,
                                "flow_in_exchange_native_units_inclusive": None,
                                "flow_net_poloniex_native_units": None,
                                "flow_out_gemini_native_units": None,
                                "flow_net_kraken_native_units": None,
                                "flow_out_exchange_native_units_inclusive": None,
                                "flow_in_gemini_native_units": None,
                                "supply_bitstamp_usd": None,
                                "flow_net_poloniex_usd": None,
                                "supply_exchange_usd": None,
                                "flow_in_bitmex_usd": None,
                                "flow_out_bitmex_usd": None,
                                "flow_in_bitfinex_native_units": None,
                                "flow_in_kraken_native_units": None,
                                "supply_binance_usd": None,
                                "flow_net_bitfinex_native_units": None,
                                "flow_net_binance_usd": None,
                                "supply_poloniex_native_units": None,
                                "supply_poloniex_usd": None,
                                "supply_bitmex_usd": None,
                                "flow_in_binance_usd": None,
                                "flow_net_bitfinex_usd": None,
                                "supply_kraken_usd": None,
                                "supply_bitfinex_native_units": None,
                                "flow_net_bitstamp_native_units": None,
                                "flow_net_gemini_usd": None,
                                "flow_out_binance_native_units": None,
                                "flow_in_bittrex_usd": None,
                                "flow_in_huobi_native_units": None,
                                "flow_in_bitfinex_usd": None,
                                "flow_out_kraken_usd": None,
                                "flow_out_bitfinex_usd": None,
                                "flow_in_huobi_usd": None,
                                "flow_in_binance_native_units": None,
                                "flow_out_huobi_native_units": None,
                                "supply_kraken_native_units": None,
                                "flow_net_huobi_native_units": None,
                                "flow_net_huobi_usd": None,
                                "flow_in_gemini_usd": None,
                                "flow_net_bitstamp_usd": None,
                                "supply_binance_native_units": None,
                                "supply_bittrex_native_units": None,
                                "supply_bitstamp_native_units": None,
                                "flow_out_bitstamp_usd": None,
                                "flow_out_kraken_native_units": None,
                                "flow_net_gemini_native_units": None,
                                "supply_bitmex_native_units": None,
                                "flow_in_bitmex_native_units": None,
                                "flow_out_bittrex_usd": None,
                                "flow_out_exchange_native_units": None,
                                "flow_out_bittrex_native_units": None,
                                "flow_out_bitstamp_native_units": None,
                                "flow_in_exchange_usd_inclusive": None,
                                "flow_out_binance_usd": None,
                                "flow_in_bitstamp_usd": None,
                                "flow_net_kraken_usd": None,
                                "supply_gemini_usd": None,
                                "flow_out_bitmex_native_units": None,
                                "supply_huobi_usd": None,
                                "flow_in_exchange_usd": None,
                                "supply_bittrex_usd": None,
                                "supply_gemini_native_units": None,
                                "flow_net_bittrex_usd": None,
                                "flow_out_exchange_usd": None,
                                "flow_in_bittrex_native_units": None,
                                "flow_in_poloniex_usd": None,
                                "supply_exchange_native_units": None,
                                "flow_out_huobi_usd": None,
                            },
                            "miner_flows": {
                                "supply_1hop_miners_native_units": None,
                                "supply_miners_native_units": None,
                                "supply_1hop_miners_usd": None,
                                "supply_miners_usd": None,
                            },
                            "name": "Compound",
                            "blockchain_stats_24_hours": {
                                "adjusted_transaction_volume": 38027017.54503817,
                                "count_of_tx": 877,
                                "transaction_volume": 43896628.06726611,
                                "new_issuance": None,
                                "average_difficulty": None,
                                "median_tx_value": 4427.126820547698,
                                "count_of_blocks_added": None,
                                "adjusted_nvt": 16.308321374092,
                                "count_of_payments": 1031,
                                "kilobytes_added": None,
                                "median_tx_fee": None,
                                "count_of_active_addresses": 479,
                            },
                            "contract_addresses": [
                                {
                                    "contract_address": "c00e94cb662c3520282e6f5717214004a7f26888.factory.bridge.near",
                                    "platform": "near-protocol",
                                }
                            ],
                            "market_data_liquidity": {
                                "asset_bid_depth": None,
                                "marketcap": None,
                                "updated_at": "0001-01-01T00:00:00Z",
                                "usd_bid_depth": None,
                                "clearing_prices_to_sell": None,
                            },
                        },
                        "status": {
                            "elapsed": 1,
                            "timestamp": "2023-08-03T18:34:22.800607564Z",
                        },
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
def messari_asset_metrics(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the asset metrics data from Messari.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-messari")
            .document(f"asset_metrics")
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


@app.get(
    "/protocols/{protocol_name}/messari-indexed-timeseries-list",
    tags=["Messari - Ecosystem"],
    #  dependencies=[Depends(get_current_user)],
    responses={
        200: {
            "description": "Indexed timeseries list",
            "content": {
                "application/json": {
                    "example": [
                        "asset_timeseries_act.addr.cnt_1d",
                        "asset_timeseries_act.addr.cnt_1w",
                        "asset_timeseries_daily.shp_1d",
                        "asset_timeseries_daily.shp_1w",
                        "asset_timeseries_daily.vol_1d",
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
def messari_indexed_timeseries_list(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the list of indexed timeseries names.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-messari")
            .document(f"indexed_timeseries_list")
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


@app.get(
    "/protocols/{protocol_name}/messari-timeseries",
    tags=["Messari - Ecosystem"],
    #  dependencies=[Depends(get_current_user)],
    responses={
        200: {
            "description": "Timeseries data",
            "content": {
                "application/json": {
                    "example": "Returns the timeseries data that is given as a parameter. Timeseries can be found in here: `https://data.messari.io/api/v1/assets/metrics`."
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
def messari_timeseries(
    protocol_name: str = Path(..., description="Protocol name"),
    timeseries_name: str = Query(..., description="Timeseries name"),
):
    """
    Returns the timeseries data that is given as a parameter.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-messari")
            .document(timeseries_name)
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
        raise HTTPException(
            status_code=500, detail=f"An error occurred {str(e)}"
        )

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data
