from datetime import datetime
import json
from typing import Union
from fastapi import FastAPI, Response
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


admin_sdk_path = os.environ['FIREBASE_ADMIN_SDK_PATH']
if not os.path.exists(admin_sdk_path):
    raise Exception(f'Admin SDK file not found in path {admin_sdk_path}')

cred = credentials.Certificate(os.environ['FIREBASE_ADMIN_SDK_PATH'])
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.environ['FIREBASE_ADMIN_SDK_PATH']

app = firebase_admin.initialize_app(cred, {
    'projectId': os.environ['FIREBASE_PROJECT_ID']
}, name='flowana_api')

db = firestore.Client()
with open('protocols.json') as f:
    protocols = json.load(f)

SECRET_KEY = os.environ['API-SECRET-KEY']
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


async def get_current_user(token: HTTPAuthorizationCredentials = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token.credentials, SECRET_KEY,
                             algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username)
    except JWTError:
        raise credentials_exception
    user = db.collection(u'users').document(username).get()
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
    "https://www.flowana-dev.vercel.app"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class UserIn(BaseModel):
    username: str
    password: str


class UserToken(BaseModel):
    username: str
    access_token: str
    token_type: str


class AdminIn(BaseModel):
    admin_username: str
    admin_password: str


@app.get("/", dependencies=[Depends(get_current_user)])
def read_root():
    return "Welcome to Flowana API!"


@app.post("/admin-create-user/")
async def admin_create_user(admin_in: AdminIn, user_in: UserIn):
    if not (admin_in.admin_username == os.environ["ADMIN-USERNAME"]
            and verify_password(admin_in.admin_password, get_password_hash(os.environ["ADMIN-PASSWORD"]))):
        raise HTTPException(
            status_code=403,
            detail="Not enough permissions",
        )

    user = db.collection(u'users').document(user_in.username).get()
    if user.exists:
        raise HTTPException(
            status_code=400,
            detail="Username already exists",
        )
    hashed_password = get_password_hash(user_in.password)
    db.collection(u'users').document(user_in.username).set({
        "username": user_in.username,
        "password": hashed_password,
    })
    return {"username": user_in.username, "password": hashed_password}


@app.post("/get-token/", response_model=UserToken)
async def login_for_access_token(user_in: UserIn):
    user = db.collection(u'users').document(user_in.username).get().to_dict()
    if user:
        if verify_password(user_in.password, user.get("password")):
            access_token_expires = datetime.timedelta(
                days=ACCESS_TOKEN_EXPIRE_DAYS)
            access_token = create_access_token(
                data={"sub": user_in.username}, expires_delta=access_token_expires
            )
            return {"username": user_in.username, "access_token": access_token, "token_type": "bearer"}
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Incorrect username or password",
        headers={"WWW-Authenticate": "Bearer"},
    )


@app.get("/test-auth", dependencies=[Depends(get_current_user)])
async def test_auth():
    return {"message": "You are authorized!"}


@app.get("/protocols/{protocol_name}/repository-info", dependencies=[Depends(get_current_user)],
         responses={
             200: {
                 "description": "Repository information",
                 "content": {
                     "application/json": {
                         "example": [
                             {
                                 "commit_comment_count": 0,
                                 "created_at": "2022-10-21T11:05:51Z",
                                 "default_branch_commit_count": 26,
                                 "description": "Swiss Army Knife for the IPFS",
                                 "disk_usage": 41555,
                                 "environment_count": 0,
                                 "fork_count": 0,
                                 "is_archived": False,
                                 "is_empty": False,
                                 "is_fork": False,
                                 "issue_count": 0,
                                 "owner_avatar_url": "https://avatars.githubusercontent.com/u/37740842?u=6fc366c4ce246b26178d89302c061bb0d4089a99&v=4",
                                 "owner_login": "justmert",
                                 "primary_language_color": "#f1e05a",
                                 "primary_language_name": "JavaScript",
                                 "pull_request_count": 0,
                                 "release_count": 0,
                                 "stargazer_count": 2}
                         ]
                     }
                 }
             },

             204: {
                 "description": "No content found",
                 "content": {
                     "application/json": {
                         "example": None
                     }
                 }
             },

             404: {
                 "description": "Not found",
                 "content": {
                     "application/json": {
                         "example": {
                                "error": "Error description"
                         }
                     }
                 }
             },
}
)
def repository_info(
    protocol_name: str = Path(..., description="Protocol name"),
    owner: str = Query(..., description="Project owner name"),
    repo: str = Query(..., description="Project repository name"),
):
    """
    Returns the last year of commit activity grouped by week. The days array is a group of commits per day, starting on Sunday.


    **_NOTE:_**  Graphql endpoint is used to get the repository information.
    """

    try:
        ref = db.collection(f'{protocol_name}-widgets').document(f'{owner}#{repo}').get(
            field_paths=['repository_info']).to_dict()

        if ref is None:
            raise exceptions.NotFound('Collection or document not found')

    except exceptions.NotFound as ex:
        # Handle case where document or collection does not exist
        raise HTTPException(
            status_code=404, detail=str(ex))

    except Exception as e:
        # Handle other exceptions
        raise HTTPException(
            status_code=404, detail=f"An error occurred {str(e)}")

    data = ref.get('repository_info', None)
    if (data is None) or (isinstance(data, dict) and not data) or (isinstance(data, list) and not data):
        raise HTTPException(
            status_code=204, detail="Content is empty.")

    return data


@app.get("/protocols/{protocol_name}/commit-activity", dependencies=[Depends(get_current_user)],
         responses={
             200: {
                 "description": "Commit activity",
                 "content": {
                     "application/json": {
                         "example": [
                             {
                                 "days": [
                                     0,
                                     3,
                                     26,
                                     20,
                                     39,
                                     1,
                                     0
                                 ],
                                 "total": 89,
                                 "week": 1336280400
                             }
                         ]
                     }
                 }
             },

             204: {
                 "description": "No content found",
                 "content": {
                     "application/json": {
                         "example": None
                     }
                 }
             },

             404: {
                 "description": "Not found",
                 "content": {
                     "application/json": {
                         "example": {
                                "error": "Error description"
                         }
                     }
                 }
             },
}
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
        ref = db.collection(f'{protocol_name}-widgets').document(f'{owner}#{repo}').get(
            field_paths=['commit_activity']).to_dict()

        if ref is None:
            raise exceptions.NotFound('Collection or document not found')

    except exceptions.NotFound as ex:
        # Handle case where document or collection does not exist
        raise HTTPException(
            status_code=404, detail=str(ex))

    except Exception as e:
        # Handle other exceptions
        raise HTTPException(
            status_code=404, detail=f"An error occurred {str(e)}")

    data = ref.get('commit_activity', None)
    if (data is None) or (isinstance(data, dict) and not data) or (isinstance(data, list) and not data):
        raise HTTPException(
            status_code=204, detail="Content is empty.")

    return data


@app.get("/protocols/{protocol_name}/participation", dependencies=[Depends(get_current_user)],
         responses={
             200: {
                 "description": "Participation",
                 "content": {
                     "application/json": {
                         "example": {
                             "xAxis": {"type": "category"},
                             "yAxis": {"type": "value"},
                             "series": [{"name": "All", "data": [3, 5, 7], "type": "line"},
                                        {"name": "Owners", "data": [
                                            1, 2, 3], "type": "line"},
                                        {"name": "Others", "data": [2, 3, 4], "type": "line"}]
                         }
                     }
                 }
             },

             204: {
                 "description": "No content found",
                 "content": {
                     "application/json": {
                         "example": None
                     }
                 }
             },

             404: {
                 "description": "Not found",
                 "content": {
                     "application/json": {
                         "example": {
                                "error": "Error description"
                         }
                     }
                 }
             },
}
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
        ref = db.collection(f'{protocol_name}-widgets').document(f'{owner}#{repo}').get(
            field_paths=['participation']).to_dict()

        if ref is None:
            raise exceptions.NotFound('Collection or document not found')

    except exceptions.NotFound as ex:
        # Handle case where document or collection does not exist
        raise HTTPException(
            status_code=404, detail=str(ex))

    except Exception as e:
        # Handle other exceptions
        raise HTTPException(
            status_code=404, detail=f"An error occurred {str(e)}")

    data = ref.get('participation', None)
    if (data is None) or (isinstance(data, dict) and not data) or (isinstance(data, list) and not data):
        raise HTTPException(
            status_code=204, detail="Content is empty.")

    return data


@app.get("/protocols/{protocol_name}/code-frequency", dependencies=[Depends(get_current_user)],
         responses={
             200: {
                 "description": "Code Frequency",
                 "content": {
                     "application/json": {
                         "example": {
                             "xAxis": {"data": ["2022-08-28", "2022-09-04", "2022-09-11"]},
                             "yAxis": {},
                             "series": [
                                {"data": [785, 12, 452],
                                    "type": "line", "stack": "x"},
                                {"data": [0, 0, -123],
                                    "type": "line", "stack": "x"}
                             ],
                         }
                     }
                 }
             },

             204: {
                 "description": "No content found",
                 "content": {
                     "application/json": {
                         "example": None
                     }
                 }
             },

             404: {
                 "description": "Not found",
                 "content": {
                     "application/json": {
                         "example": {
                                "error": "Error description"
                         }
                     }
                 }
             },
}
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
        ref = db.collection(f'{protocol_name}-widgets').document(f'{owner}#{repo}').get(
            field_paths=['code_frequency']).to_dict()

        if ref is None:
            raise exceptions.NotFound('Collection or document not found')

    except exceptions.NotFound as ex:
        # Handle case where document or collection does not exist
        raise HTTPException(
            status_code=404, detail=str(ex))

    except Exception as e:
        # Handle other exceptions
        raise HTTPException(
            status_code=404, detail=f"An error occurred {str(e)}")

    data = ref.get('code_frequency', None)
    if (data is None) or (isinstance(data, dict) and not data) or (isinstance(data, list) and not data):
        raise HTTPException(
            status_code=204, detail="Content is empty.")

    return data


@app.get("/protocols/{protocol_name}/punch-card", dependencies=[Depends(get_current_user)],
         responses={
             200: {
                 "description": "Punch Card",
                 "content": {
                     "application/json": {
                         "example": [{
                             'day': 0,
                             'hour': 0,
                             'commits': 4
                         },
                             {
                             'day': 0,
                             'hour': 1,
                             'commits': 24
                         }]
                     }
                 }
             },

             204: {
                 "description": "No content found",
                 "content": {
                     "application/json": {
                         "example": None
                     }
                 }
             },

             404: {
                 "description": "Not found",
                 "content": {
                     "application/json": {
                         "example": {
                                "error": "Error description"
                         }
                     }
                 }
             },
}
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
        ref = db.collection(f'{protocol_name}-widgets').document(f'{owner}#{repo}').get(
            field_paths=['punch_card']).to_dict()

        if ref is None:
            raise exceptions.NotFound('Collection or document not found')

    except exceptions.NotFound as ex:
        # Handle case where document or collection does not exist
        raise HTTPException(
            status_code=404, detail=str(ex))

    except Exception as e:
        # Handle other exceptions
        raise HTTPException(
            status_code=404, detail=f"An error occurred {str(e)}")

    data = ref.get('punch_card', None)
    if (data is None) or (isinstance(data, dict) and not data) or (isinstance(data, list) and not data):
        raise HTTPException(
            status_code=204, detail="Content is empty.")

    return data


@app.get("/protocols/{protocol_name}/community-profile", dependencies=[Depends(get_current_user)],
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
                                     "html_url": "https://github.com/octocat/Hello-World/blob/master/CODE_OF_CONDUCT.md"
                                 },
                                 "code_of_conduct_file": {
                                     "url": "https://api.github.com/repos/octocat/Hello-World/contents/CODE_OF_CONDUCT.md",
                                     "html_url": "https://github.com/octocat/Hello-World/blob/master/CODE_OF_CONDUCT.md"
                                 },
                                 "contributing": {
                                     "url": "https://api.github.com/repos/octocat/Hello-World/contents/CONTRIBUTING",
                                     "html_url": "https://github.com/octocat/Hello-World/blob/master/CONTRIBUTING"
                                 },
                                 "issue_template": {
                                     "url": "https://api.github.com/repos/octocat/Hello-World/contents/ISSUE_TEMPLATE",
                                     "html_url": "https://github.com/octocat/Hello-World/blob/master/ISSUE_TEMPLATE"
                                 },
                                 "pull_request_template": {
                                     "url": "https://api.github.com/repos/octocat/Hello-World/contents/PULL_REQUEST_TEMPLATE",
                                     "html_url": "https://github.com/octocat/Hello-World/blob/master/PULL_REQUEST_TEMPLATE"
                                 },
                                 "license": {
                                     "name": "MIT License",
                                     "key": "mit",
                                     "spdx_id": "MIT",
                                     "url": "https://api.github.com/licenses/mit",
                                     "html_url": "https://github.com/octocat/Hello-World/blob/master/LICENSE",
                                     "node_id": "MDc6TGljZW5zZW1pdA=="
                                 },
                                 "readme": {
                                     "url": "https://api.github.com/repos/octocat/Hello-World/contents/README.md",
                                     "html_url": "https://github.com/octocat/Hello-World/blob/master/README.md"
                                 }
                             },
                             "updated_at": "2017-02-28T19:09:29Z",
                             "content_reports_enabled": None
                         }

                     }
                 }
             },

             204: {
                 "description": "No content found",
                 "content": {
                     "application/json": {
                         "example": None
                     }
                 }
             },

             404: {
                 "description": "Not found",
                 "content": {
                     "application/json": {
                         "example": {
                                "error": "Error description"
                         }
                     }
                 }
             },
}
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
        ref = db.collection(f'{protocol_name}-widgets').document(f'{owner}#{repo}').get(
            field_paths=['community_profile']).to_dict()

        if ref is None:
            raise exceptions.NotFound('Collection or document not found')

    except exceptions.NotFound as ex:
        # Handle case where document or collection does not exist
        raise HTTPException(
            status_code=404, detail=str(ex))

    except Exception as e:
        # Handle other exceptions
        raise HTTPException(
            status_code=404, detail=f"An error occurred {str(e)}")

    data = ref.get('community_profile', None)
    if (data is None) or (isinstance(data, dict) and not data) or (isinstance(data, list) and not data):
        raise HTTPException(
            status_code=204, detail="Content is empty.")

    return data


@app.get("/protocols/{protocol_name}/language-breakdown", dependencies=[Depends(get_current_user)],
         responses={
             200: {
                 "description": "Language breakdown",
                 "content": {
                     "application/json": {
                         "example": [{
                             "name": "JavaScript",
                             "percentage": 100,
                             "size": 55852
                         }]
                     }
                 }
             },

             204: {
                 "description": "No content found",
                 "content": {
                     "application/json": {
                         "example": None
                     }
                 }
             },

             404: {
                 "description": "Not found",
                 "content": {
                     "application/json": {
                         "example": {
                                "error": "Error description"
                         }
                     }
                 }
             },
}
)
def language_breakdown(
    protocol_name: str = Path(..., description="Protocol name"),
    owner: str = Query(..., description="Project owner name"),
    repo: str = Query(..., description="Project repository name"),
):
    """
    Returns the language breakdown for the repository.

    **_NOTE:_**  Graphql endpoint is used to get the language breakdown.
    """

    try:
        ref = db.collection(f'{protocol_name}-widgets').document(f'{owner}#{repo}').get(
            field_paths=['language_breakdown']).to_dict()

        if ref is None:
            raise exceptions.NotFound('Collection or document not found')

    except exceptions.NotFound as ex:
        # Handle case where document or collection does not exist
        raise HTTPException(
            status_code=404, detail=str(ex))

    except Exception as e:
        # Handle other exceptions
        raise HTTPException(
            status_code=404, detail=f"An error occurred {str(e)}")

    data = ref.get('language_breakdown', None)
    if (data is None) or (isinstance(data, dict) and not data) or (isinstance(data, list) and not data):
        raise HTTPException(
            status_code=204, detail="Content is empty.")

    return data


@app.get("/protocols/{protocol_name}/issue-count", dependencies=[Depends(get_current_user)],
         responses={
             200: {
                 "description": "Issue count",
                 "content": {
                     "application/json": {
                         "example": {
                             'closed': 12,
                             'open': 2
                         }
                     }
                 }
             },

             204: {
                 "description": "No content found",
                 "content": {
                     "application/json": {
                         "example": None
                     }
                 }
             },

             404: {
                 "description": "Not found",
                 "content": {
                     "application/json": {
                         "example": {
                                "error": "Error description"
                         }
                     }
                 }
             },
}
)
def issue_count(
    protocol_name: str = Path(..., description="Protocol name"),
    owner: str = Query(..., description="Project owner name"),
    repo: str = Query(..., description="Project repository name"),
):
    """
    Returns the open/closed issue count for the repository.

    **_NOTE:_**  Graphql endpoint is used to get the issue count.
    """

    try:
        ref = db.collection(f'{protocol_name}-widgets').document(f'{owner}#{repo}').get(
            field_paths=['issue_count']).to_dict()

        if ref is None:
            raise exceptions.NotFound('Collection or document not found')

    except exceptions.NotFound as ex:
        # Handle case where document or collection does not exist
        raise HTTPException(
            status_code=404, detail=str(ex))

    except Exception as e:
        # Handle other exceptions
        raise HTTPException(
            status_code=404, detail=f"An error occurred {str(e)}")

    data = ref.get('issue_count', None)
    if (data is None) or (isinstance(data, dict) and not data) or (isinstance(data, list) and not data):
        raise HTTPException(
            status_code=204, detail="Content is empty.")

    return data


@app.get("/protocols/{protocol_name}/most-active-issues", dependencies=[Depends(get_current_user)],
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
                 }
             },

             204: {
                 "description": "No content found",
                 "content": {
                     "application/json": {
                         "example": None
                     }
                 }
             },

             404: {
                 "description": "Not found",
                 "content": {
                     "application/json": {
                         "example": {
                                "error": "Error description"
                         }
                     }
                 }
             },
}
)
def most_active_issues(
    protocol_name: str = Path(..., description="Protocol name"),
    owner: str = Query(..., description="Project owner name"),
    repo: str = Query(..., description="Project repository name"),
    interval: str = Query(
        ..., description="Interval for which the most active issues are to be returned")
):
    """
    Returns the most active issues for the repository.

    `interval` can be one of the following: day, week, month, year

    **_NOTE:_**  Graphql endpoint is used to get the most active issues.
    """

    try:
        ref = db.collection(f'{protocol_name}-widgets').document(f'{owner}#{repo}').get(
            field_paths=['most_active_issues']).to_dict()

        if ref is None:
            raise exceptions.NotFound('Collection or document not found')

    except exceptions.NotFound as ex:
        # Handle case where document or collection does not exist
        raise HTTPException(
            status_code=404, detail=str(ex))

    except Exception as e:
        # Handle other exceptions
        raise HTTPException(
            status_code=404, detail=f"An error occurred {str(e)}")

    data = ref.get('most_active_issues', None)
    if (data is None) or (isinstance(data, dict) and not data) or (isinstance(data, list) and not data):
        raise HTTPException(
            status_code=204, detail="Content is empty.")

    if interval.strip().lower() not in ['day', 'week', 'month', 'year']:
        raise HTTPException(
            status_code=400, detail="Invalid interval value")

    interval_data = data.get(interval.strip().lower(), [])
    if not interval_data:
        raise HTTPException(
            status_code=204, detail="Content is empty.")

    return interval_data


@app.get("/protocols/{protocol_name}/pull-request-count", dependencies=[Depends(get_current_user)],
         responses={
             200: {
                 "description": "Pull request count",
                 "content": {
                     "application/json": {
                         "example": {
                             'closed': 12,
                             'open': 2
                         }
                     }
                 }
             },

             204: {
                 "description": "No content found",
                 "content": {
                     "application/json": {
                         "example": None
                     }
                 }
             },

             404: {
                 "description": "Not found",
                 "content": {
                     "application/json": {
                         "example": {
                                "error": "Error description"
                         }
                     }
                 }
             },
}
)
def pull_request_count(
    protocol_name: str = Path(..., description="Protocol name"),
    owner: str = Query(..., description="Project owner name"),
    repo: str = Query(..., description="Project repository name"),
):
    """
    Returns the pull request count for the repository.

    **_NOTE:_**  Graphql endpoint is used to get the pull request count.
    """

    try:
        ref = db.collection(f'{protocol_name}-widgets').document(f'{owner}#{repo}').get(
            field_paths=['pull_request_count']).to_dict()

        if ref is None:
            raise exceptions.NotFound('Collection or document not found')

    except exceptions.NotFound as ex:
        # Handle case where document or collection does not exist
        raise HTTPException(
            status_code=404, detail=str(ex))

    except Exception as e:
        # Handle other exceptions
        raise HTTPException(
            status_code=404, detail=f"An error occurred {str(e)}")

    data = ref.get('pull_request_count', None)
    if (data is None) or (isinstance(data, dict) and not data) or (isinstance(data, list) and not data):
        raise HTTPException(
            status_code=204, detail="Content is empty.")

    return data


# @DeprecationWarning
# @app.get("/protocols/{protocol_name}/pull-request-activity")
# def pull_request_activity(protocol_name: str, owner: str, repo: str):
#     """
#     Deprecated
#     """
#     pass


# @DeprecationWarning
# @app.get("/protocols/{protocol_name}/issue-activity")
# def issue_activity(protocol_name: str, owner: str, repo: str):
#     """
#     Deprecated
#     """
#     pass

# @DeprecationWarning
# @app.get("/protocols/{protocol_name}/most-active-pull-requests")
# def most_active_pull_requests(protocol_name: str, owner: str, repo: str):
#     """
#     Deprecated
#     """
#     pass


@app.get("/protocols/{protocol_name}/recent-issues", dependencies=[Depends(get_current_user)],
         responses={
             200: {
                 "description": "Recent issues",
                 "content": {
                     "application/json": {
                         "example": [{
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
                         }]
                     }
                 }
             },

             204: {
                 "description": "No content found",
                 "content": {
                     "application/json": {
                         "example": None
                     }
                 }
             },

             404: {
                 "description": "Not found",
                 "content": {
                     "application/json": {
                         "example": {
                                "error": "Error description"
                         }
                     }
                 }
             },
}
)
def recent_issues(
    protocol_name: str = Path(..., description="Protocol name"),
    owner: str = Query(..., description="Project owner name"),
    repo: str = Query(..., description="Project repository name"),
    order_by: str = Query(..., description="Order by field"),
):
    """
    Returns the recent issues for the repository.

    `order_by` can be one of the following values: CREATED_AT, UPDATED_AT


    **_NOTE:_**  Graphql endpoint is used to get the recent issues.
    """

    try:
        if order_by.strip().lower() not in ['created_at', 'updated_at']:
            raise HTTPException(
                status_code=400, detail="Invalid order by value")

        if order_by.strip().lower() == 'created_at':
            field_name = 'recent_created_issues'

        elif order_by.strip().lower() == 'updated_at':
            field_name = 'recent_updated_issues'

        collection_ref = db.collection(f'{protocol_name}-widgets').document(f'{owner}#{repo}').get(
            field_paths=[field_name]).to_dict()

        if collection_ref is None:
            raise exceptions.NotFound('Collection or document not found')

    except exceptions.NotFound as ex:
        # Handle case where document or collection does not exist
        raise HTTPException(
            status_code=404, detail=str(ex))

    except Exception as e:
        # Handle other exceptions
        raise HTTPException(
            status_code=404, detail=f"An error occurred {str(e)}")

    data = collection_ref.get(field_name, None)

    if (data is None) or (isinstance(data, dict) and not data) or (isinstance(data, list) and not data):
        raise HTTPException(
            status_code=204, detail="Content is empty.")

    return data


@app.get("/protocols/{protocol_name}/recent-pull-requests", dependencies=[Depends(get_current_user)],
         responses={
             200: {
                 "description": "Recent pull requests",
                 "content": {
                     "application/json": {
                         "example": [{
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

                         }]
                     }
                 }
             },

             204: {
                 "description": "No content found",
                 "content": {
                     "application/json": {
                         "example": None
                     }
                 }
             },

             404: {
                 "description": "Not found",
                 "content": {
                     "application/json": {
                         "example": {
                                "error": "Error description"
                         }
                     }
                 }
             },
}
)
def recent_pull_requests(
    protocol_name: str = Path(..., description="Protocol name"),
    owner: str = Query(..., description="Project owner name"),
    repo: str = Query(..., description="Project repository name"),
    order_by: str = Query(..., description="Order by field"),
):
    """
    Returns the recent pull requests for the repository.

    order_by can be one of the following values: CREATED_AT, UPDATED_AT


    **_NOTE:_**  Graphql endpoint is used to get the recent pull requests.
    """

    try:
        if order_by.strip().lower() not in ['created_at', 'updated_at']:
            raise HTTPException(
                status_code=400, detail="Invalid order by value")

        if order_by.strip().lower() == 'created_at':
            field_name = 'recent_created_pull_requests'

        elif order_by.strip().lower() == 'updated_at':
            field_name = 'recent_updated_pull_requests'

        collection_ref = db.collection(f'{protocol_name}-widgets').document(f'{owner}#{repo}').get(
            field_paths=[field_name]).to_dict()

        if collection_ref is None:
            raise exceptions.NotFound('Collection or document not found')

    except exceptions.NotFound as ex:
        # Handle case where document or collection does not exist
        raise HTTPException(
            status_code=404, detail=str(ex))

    except Exception as e:
        # Handle other exceptions
        raise HTTPException(
            status_code=404, detail=f"An error occurred {str(e)}")

    data = collection_ref.get(field_name, None)

    if (data is None) or (isinstance(data, dict) and not data) or (isinstance(data, list) and not data):
        raise HTTPException(
            status_code=204, detail="Content is empty.")

    return data


@app.get("/protocols/{protocol_name}/recent-stargazing-activity", dependencies=[Depends(get_current_user)],
         responses={
             200: {
                 "description": "Recent stargazing activity",
                 "content": {
                     "application/json": {
                         "example": {
                             "xAxis": {"data": [91, 135, 143],
                                       "yAxis": {},
                                       "series": [
                                {"data": ['2023-04-16', '2023-04-23', '2023-04-30'],
                                 "type": "line", "stack": "x"},
                             ],
                             }
                         }
                     }
                 }
             },

             204: {
                 "description": "No content found",
                 "content": {
                     "application/json": {
                         "example": None
                     }
                 }
             },

             404: {
                 "description": "Not found",
                 "content": {
                     "application/json": {
                         "example": {
                                "error": "Error description"
                         }
                     }
                 }
             },
}
)
def recent_stargazing_activity(
    protocol_name: str = Path(..., description="Protocol name"),
    owner: str = Query(..., description="Project owner name"),
    repo: str = Query(..., description="Project repository name"),
):
    """
    Returns the recent stargazing activity in Apache e-chart format. Fetches max_fetch_page (currently 15) * 100 stargazers and if last star date - first star date is more than 6 months, then chart interval will be month based. If less than 6 months and more than 1 month, then chart interval will be week based. If less than 1 month, then chart interval will be day based.

    **_NOTE:_**  Graphql endpoint is used to get the recent stargazing activity.
    """

    try:
        ref = db.collection(f'{protocol_name}-widgets').document(f'{owner}#{repo}').get(
            field_paths=['recent_stargazing_activity']).to_dict()

        if ref is None:
            raise exceptions.NotFound('Collection or document not found')

    except exceptions.NotFound as ex:
        # Handle case where document or collection does not exist
        raise HTTPException(
            status_code=404, detail=str(ex))

    except Exception as e:
        # Handle other exceptions
        raise HTTPException(
            status_code=404, detail=f"An error occurred {str(e)}")

    data = ref.get('recent_stargazing_activity', None)
    if (data is None) or (isinstance(data, dict) and not data) or (isinstance(data, list) and not data) or \
            (not data['series'][0]['data']):
        raise HTTPException(
            status_code=204, detail="Content is empty.")

    return data


@app.get("/protocols/{protocol_name}/recent-commits", dependencies=[Depends(get_current_user)],
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
                 }
             },

             204: {
                 "description": "No content found",
                 "content": {
                     "application/json": {
                         "example": None
                     }
                 }
             },

             404: {
                 "description": "Not found",
                 "content": {
                     "application/json": {
                         "example": {
                                "error": "Error description"
                         }
                     }
                 }
             },
}
)
def recent_commits(
    protocol_name: str = Path(..., description="Protocol name"),
    owner: str = Query(..., description="Project owner name"),
    repo: str = Query(..., description="Project repository name"),
):
    """
    Returns the recent commits for the repository. The author is selected as the one who has committed the code. Is the author is not available, then the committer is selected.

    **_NOTE:_**  Graphql endpoint is used to get the recent commits.
    """

    try:
        ref = db.collection(f'{protocol_name}-widgets').document(f'{owner}#{repo}').get(
            field_paths=['recent_commits']).to_dict()

        if ref is None:
            raise exceptions.NotFound('Collection or document not found')

    except exceptions.NotFound as ex:
        # Handle case where document or collection does not exist
        raise HTTPException(
            status_code=404, detail=str(ex))

    except Exception as e:
        # Handle other exceptions
        raise HTTPException(
            status_code=404, detail=f"An error occurred {str(e)}")

    data = ref.get('recent_commits', None)
    if (data is None) or (isinstance(data, dict) and not data) or (isinstance(data, list) and not data) or \
            (not data):
        raise HTTPException(
            status_code=204, detail="Content is empty.")

    return data


@app.get("/protocols/{protocol_name}/recent-releases", dependencies=[Depends(get_current_user)],
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
                 }
             },

             204: {
                 "description": "No content found.",
                 "content": {
                     "application/json": {
                         "example": None
                     }
                 }
             },

             404: {
                 "description": "Not found",
                 "content": {
                     "application/json": {
                         "example": {
                                "error": "Error description"
                         }
                     }
                 }
             },
}
)
def recent_releases(
    protocol_name: str = Path(..., description="Protocol name"),
    owner: str = Query(..., description="Project owner name"),
    repo: str = Query(..., description="Project repository name"),
):
    """
    Returns the recent commits for the repository. The author is selected as the one who has committed the code. Is the author is not available, then the committer is selected.

    **_NOTE:_**  Graphql endpoint is used to get the recent releases.
    """

    try:
        ref = db.collection(f'{protocol_name}-widgets').document(f'{owner}#{repo}').get(
            field_paths=['recent_releases']).to_dict()

        if ref is None:
            raise exceptions.NotFound('Collection or document not found')

    except exceptions.NotFound as ex:
        # Handle case where document or collection does not exist
        raise HTTPException(
            status_code=404, detail=str(ex))

    except Exception as e:
        # Handle other exceptions
        raise HTTPException(
            status_code=404, detail=f"An error occurred {str(e)}")

    data = ref.get('recent_releases ', None)
    if (data is None) or (isinstance(data, dict) and not data) or (isinstance(data, list) and not data) or \
            (not data):
        raise HTTPException(
            status_code=204, detail="Content is empty.")

    return data


@app.get("/protocols/{protocol_name}/cumulative-stats", dependencies=[Depends(get_current_user)],
         responses={
             200: {
                 "description": "Cumulative Stats",
                 "content": {
                     "application/json": {
                         "example": [
                             {
                                'commit_comment_count': 6957,
                                'default_branch_commit_count': 9333,
                                'disk_usage': 52013,
                                'environment_count': 17,
                                'fork_count': 2174,
                                'issue_count': 1302,
                                'pull_request_count': 2974,
                                'release_count': 19,
                                'stargazers_count': 0,
                                'watcher_count': 178
                             }
                         ]
                     }
                 }
             },

             204: {
                 "description": "No content found.",
                 "content": {
                     "application/json": {
                         "example": None
                     }
                 }
             },

             404: {
                 "description": "Not found",
                 "content": {
                     "application/json": {
                         "example": {
                                "error": "Error description"
                         }
                     }
                 }
             },
}
)
def cumulative_stats(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the cumulative stats for the protocol.

    """

    try:
        ref = db.collection(f'{protocol_name}-cumulative').document(f'cumulative_info').get(
            field_paths=['data']).to_dict()

        if ref is None:
            raise exceptions.NotFound('Collection or document not found')

    except exceptions.NotFound as ex:
        # Handle case where document or collection does not exist
        raise HTTPException(
            status_code=404, detail=str(ex))

    except Exception as e:
        # Handle other exceptions
        raise HTTPException(
            status_code=404, detail=f"An error occurred {str(e)}")

    data = ref.get('data', None)
    if (data is None) or (isinstance(data, dict) and not data) or (isinstance(data, list) and not data) or \
            (not data):
        raise HTTPException(
            status_code=204, detail="Content is empty.")

    return data


@app.get("/protocols/{protocol_name}/cumulative-commit-activity", dependencies=[Depends(get_current_user)],
         responses={
             200: {
                 "description": "Cumulative Commit Activity",
                 "content": {
                     "application/json": {
                         "example": [
                             {
                                 "days": [
                                     0,
                                     3,
                                     26,
                                     20,
                                     39,
                                     1,
                                     0
                                 ],
                                 "total": 89,
                                 "week": 1336280400
                             }]
                     }
                 }
             },

             204: {
                 "description": "No content found.",
                 "content": {
                     "application/json": {
                         "example": None
                     }
                 }
             },

             404: {
                 "description": "Not found",
                 "content": {
                     "application/json": {
                         "example": {
                                "error": "Error description"
                         }
                     }
                 }
             },
}
)
def cumulative_commit_activity(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the cumulative commit activity for the protocol.

    """

    try:
        ref = db.collection(f'{protocol_name}-cumulative').document(f'cumulative_commit_activity').get(
            field_paths=['data']).to_dict()

        if ref is None:
            raise exceptions.NotFound('Collection or document not found')

    except exceptions.NotFound as ex:
        # Handle case where document or collection does not exist
        raise HTTPException(
            status_code=404, detail=str(ex))

    except Exception as e:
        # Handle other exceptions
        raise HTTPException(
            status_code=404, detail=f"An error occurred {str(e)}")

    data = ref.get('data', None)
    if (data is None) or (isinstance(data, dict) and not data) or (isinstance(data, list) and not data) or \
            (not data):
        raise HTTPException(
            status_code=204, detail="Content is empty.")

    return data


@app.get("/protocols/{protocol_name}/cumulative-participation", dependencies=[Depends(get_current_user)],
         responses={
             200: {
                 "description": "Cumulative Participation",
                 "content": {
                     "application/json": {
                         "example": [
                            {
                                "xAxis": {"type": "category"},
                                "yAxis": {"type": "value"},
                                "series": [{"name": "All", "data": [3, 5, 7], "type": "line"},
                                           {"name": "Owners", "data": [
                                            1, 2, 3], "type": "line"},
                                           {"name": "Others", "data": [2, 3, 4], "type": "line"}]
                            }]
                     }
                 }
             },

             204: {
                 "description": "No content found.",
                 "content": {
                     "application/json": {
                         "example": None
                     }
                 }
             },

             404: {
                 "description": "Not found",
                 "content": {
                     "application/json": {
                         "example": {
                                "error": "Error description"
                         }
                     }
                 }
             },
}
)
def cumulative_participation(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the cumulative participation for the protocol.

    """

    try:
        ref = db.collection(f'{protocol_name}-cumulative').document(f'cumulative_participation').get(
            field_paths=['data']).to_dict()

        if ref is None:
            raise exceptions.NotFound('Collection or document not found')

    except exceptions.NotFound as ex:
        # Handle case where document or collection does not exist
        raise HTTPException(
            status_code=404, detail=str(ex))

    except Exception as e:
        # Handle other exceptions
        raise HTTPException(
            status_code=404, detail=f"An error occurred {str(e)}")

    data = ref.get('data', None)
    if (data is None) or (isinstance(data, dict) and not data) or (isinstance(data, list) and not data) or \
            (not data):
        raise HTTPException(
            status_code=204, detail="Content is empty.")

    return data


@app.get("/protocols/{protocol_name}/cumulative-code-frequency", dependencies=[Depends(get_current_user)],
         responses={
             200: {
                 "description": "Cumulative Code Frequency",
                 "content": {
                     "application/json": {
                         "example": [
                            {
                                "xAxis": {"type": "category"},
                                "yAxis": {"type": "value"},
                                "series": [{"name": "All", "data": [3, 5, 7], "type": "line"},
                                           {"name": "Owners", "data": [
                                            1, 2, 3], "type": "line"},
                                           {"name": "Others", "data": [2, 3, 4], "type": "line"}]
                            }]
                     }
                 }
             },

             204: {
                 "description": "No content found.",
                 "content": {
                     "application/json": {
                         "example": None
                     }
                 }
             },

             404: {
                 "description": "Not found",
                 "content": {
                     "application/json": {
                         "example": {
                                "error": "Error description"
                         }
                     }
                 }
             },
}
)
def cumulative_code_frequency(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the cumulative code frequency for the protocol.

    """

    try:
        ref = db.collection(f'{protocol_name}-cumulative').document(f'cumulative_code_frequency').get(
            field_paths=['data']).to_dict()

        if ref is None:
            raise exceptions.NotFound('Collection or document not found')

    except exceptions.NotFound as ex:
        # Handle case where document or collection does not exist
        raise HTTPException(
            status_code=404, detail=str(ex))

    except Exception as e:
        # Handle other exceptions
        raise HTTPException(
            status_code=404, detail=f"An error occurred {str(e)}")

    data = ref.get('data', None)
    if (data is None) or (isinstance(data, dict) and not data) or (isinstance(data, list) and not data) or \
            (not data):
        raise HTTPException(
            status_code=204, detail="Content is empty.")

    return data


@app.get("/protocols/{protocol_name}/cumulative-punch-card", dependencies=[Depends(get_current_user)],
         responses={
             200: {
                 "description": "Cumulative Punch Card",
                 "content": {
                     "application/json": {
                         "example": [{
                             'day': 0,
                             'hour': 0,
                             'commits': 4
                         },
                             {
                             'day': 0,
                             'hour': 1,
                             'commits': 24
                         }]
                     }
                 }
             },

             204: {
                 "description": "No content found.",
                 "content": {
                     "application/json": {
                         "example": None
                     }
                 }
             },

             404: {
                 "description": "Not found",
                 "content": {
                     "application/json": {
                         "example": {
                                "error": "Error description"
                         }
                     }
                 }
             },
}
)
def cumulative_punch_card(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the cumulative punch card for the protocol.

    """

    try:
        ref = db.collection(f'{protocol_name}-cumulative').document(f'cumulative_punch_card').get(
            field_paths=['data']).to_dict()

        if ref is None:
            raise exceptions.NotFound('Collection or document not found')

    except exceptions.NotFound as ex:
        # Handle case where document or collection does not exist
        raise HTTPException(
            status_code=404, detail=str(ex))

    except Exception as e:
        # Handle other exceptions
        raise HTTPException(
            status_code=404, detail=f"An error occurred {str(e)}")

    data = ref.get('data', None)
    if (data is None) or (isinstance(data, dict) and not data) or (isinstance(data, list) and not data) or \
            (not data):
        raise HTTPException(
            status_code=204, detail="Content is empty.")

    return data


@app.get("/protocols/{protocol_name}/cumulative-language-breakdown", dependencies=[Depends(get_current_user)],
         responses={
             200: {
                 "description": "Cumulative Language Breakdown",
                 "content": {
                     "application/json": {
                         "example": [{
                             "name": "JavaScript",
                             "percentage": 100,
                             "size": 55852
                         }]
                     }
                 }
             },

             204: {
                 "description": "No content found.",
                 "content": {
                     "application/json": {
                         "example": None
                     }
                 }
             },

             404: {
                 "description": "Not found",
                 "content": {
                     "application/json": {
                         "example": {
                                "error": "Error description"
                         }
                     }
                 }
             },
}
)
def cumulative_language_breakdown(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the cumulative language breakdown for the protocol.

    """

    try:
        ref = db.collection(f'{protocol_name}-cumulative').document(f'cumulative_language_breakdown').get(
            field_paths=['data']).to_dict()

        if ref is None:
            raise exceptions.NotFound('Collection or document not found')

    except exceptions.NotFound as ex:
        # Handle case where document or collection does not exist
        raise HTTPException(
            status_code=404, detail=str(ex))

    except Exception as e:
        # Handle other exceptions
        raise HTTPException(
            status_code=404, detail=f"An error occurred {str(e)}")

    data = ref.get('data', None)
    if (data is None) or (isinstance(data, dict) and not data) or (isinstance(data, list) and not data) or \
            (not data):
        raise HTTPException(
            status_code=204, detail="Content is empty.")

    return data


@app.get("/protocols/{protocol_name}/cumulative-issue-count", dependencies=[Depends(get_current_user)],
         responses={
             200: {
                 "description": "Cumulative Issue Count",
                 "content": {
                     "application/json": {
                         "example": {
                             'closed': 12,
                             'open': 2
                         }
                     }
                 }
             },

             204: {
                 "description": "No content found.",
                 "content": {
                     "application/json": {
                         "example": None
                     }
                 }
             },

             404: {
                 "description": "Not found",
                 "content": {
                     "application/json": {
                         "example": {
                                "error": "Error description"
                         }
                     }
                 }
             },
}
)
def cumulative_issue_count(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the cumulative issue count for the protocol.

    """

    try:
        ref = db.collection(f'{protocol_name}-cumulative').document(f'cumulative_issue_count').get(
            field_paths=['data']).to_dict()

        if ref is None:
            raise exceptions.NotFound('Collection or document not found')

    except exceptions.NotFound as ex:
        # Handle case where document or collection does not exist
        raise HTTPException(
            status_code=404, detail=str(ex))

    except Exception as e:
        # Handle other exceptions
        raise HTTPException(
            status_code=404, detail=f"An error occurred {str(e)}")

    data = ref.get('data', None)
    if (data is None) or (isinstance(data, dict) and not data) or (isinstance(data, list) and not data) or \
            (not data):
        raise HTTPException(
            status_code=204, detail="Content is empty.")

    return data


@app.get("/protocols/{protocol_name}/cumulative-most-active-issues", dependencies=[Depends(get_current_user)],
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
                                    "repo": "lenster"
                                }
                         ]
                     }
                 }
             },

             204: {
                 "description": "No content found.",
                 "content": {
                     "application/json": {
                         "example": None
                     }
                 }
             },

             404: {
                 "description": "Not found",
                 "content": {
                     "application/json": {
                         "example": {
                                "error": "Error description"
                         }
                     }
                 }
             },
}
)
def cumulative_most_active_issues(
    protocol_name: str = Path(..., description="Protocol name"),
    interval: str = Query(
        ..., description="Interval for which the most active issues are to be returned")

):
    """
    Returns the cumulative most active issues for the protocol.

    `interval` can be one of the following: day, week, month, year


    """

    try:
        ref = db.collection(f'{protocol_name}-cumulative').document(f'cumulative_most_active_issues').get(
            field_paths=['data']).to_dict()

        if ref is None:
            raise exceptions.NotFound('Collection or document not found')

    except exceptions.NotFound as ex:
        # Handle case where document or collection does not exist
        raise HTTPException(
            status_code=404, detail=str(ex))

    except Exception as e:
        # Handle other exceptions
        raise HTTPException(
            status_code=404, detail=f"An error occurred {str(e)}")

    data = ref.get('data', None)
    if (data is None) or (isinstance(data, dict) and not data) or (isinstance(data, list) and not data) or \
            (not data):
        raise HTTPException(
            status_code=204, detail="Content is empty.")

    if interval.strip().lower() not in ['day', 'week', 'month', 'year']:
        raise HTTPException(
            status_code=400, detail="Invalid interval value")

    interval_data = data.get(interval.strip().lower(), [])
    if not interval_data:
        raise HTTPException(
            status_code=204, detail="Content is empty.")

    return interval_data


@app.get("/protocols/{protocol_name}/cumulative-pull-request-count", dependencies=[Depends(get_current_user)],
         responses={
             200: {
                 "description": "Cumulative Pull Request Count",
                 "content": {
                     "application/json": {
                         "example": {
                             'closed': 12,
                             'open': 2
                         }
                     }
                 }
             },

             204: {
                 "description": "No content found.",
                 "content": {
                     "application/json": {
                         "example": None
                     }
                 }
             },

             404: {
                 "description": "Not found",
                 "content": {
                     "application/json": {
                         "example": {
                                "error": "Error description"
                         }
                     }
                 }
             },
}
)
def cumulative_pull_request_count(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the cumulative pull request count for the protocol.

    """

    try:
        ref = db.collection(f'{protocol_name}-cumulative').document(f'cumulative_pull_request_count').get(
            field_paths=['data']).to_dict()

        if ref is None:
            raise exceptions.NotFound('Collection or document not found')

    except exceptions.NotFound as ex:
        # Handle case where document or collection does not exist
        raise HTTPException(
            status_code=404, detail=str(ex))

    except Exception as e:
        # Handle other exceptions
        raise HTTPException(
            status_code=404, detail=f"An error occurred {str(e)}")

    data = ref.get('data', None)
    if (data is None) or (isinstance(data, dict) and not data) or (isinstance(data, list) and not data) or \
            (not data):
        raise HTTPException(
            status_code=204, detail="Content is empty.")

    return data


@app.get("/protocols/{protocol_name}/cumulative-recent-issues", dependencies=[Depends(get_current_user)],
         responses={
             200: {
                 "description": "Cumulative Recent Issues",
                 "content": {
                     "application/json": {
                         "example": [{
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
                         }]
                     }
                 }
             },

             204: {
                 "description": "No content found.",
                 "content": {
                     "application/json": {
                         "example": None
                     }
                 }
             },

             404: {
                 "description": "Not found",
                 "content": {
                     "application/json": {
                         "example": {
                                "error": "Error description"
                         }
                     }
                 }
             },
}
)
def cumulative_recent_issues(
    protocol_name: str = Path(..., description="Protocol name"),
    order_by: str = Query(..., description="Order by field"),
):
    """
    Returns the cumulative recent issues for the protocol.

    `order_by` can be one of the following values: CREATED_AT, UPDATED_AT

    """

    try:
        if order_by.strip().lower() not in ['created_at', 'updated_at']:
            raise HTTPException(
                status_code=400, detail="Invalid order by value")

        if order_by.strip().lower() == 'created_at':
            field_name = 'recent_created_issues'

        elif order_by.strip().lower() == 'updated_at':
            field_name = 'recent_updated_issues'

        ref = db.collection(f'{protocol_name}-cumulative').document(f'cumulative_{field_name}').get(
            field_paths=['data']).to_dict()

        if ref is None:
            raise exceptions.NotFound('Collection or document not found')

    except exceptions.NotFound as ex:
        # Handle case where document or collection does not exist
        raise HTTPException(
            status_code=404, detail=str(ex))

    except Exception as e:
        # Handle other exceptions
        raise HTTPException(
            status_code=404, detail=f"An error occurred {str(e)}")

    data = ref.get('data', None)
    if (data is None) or (isinstance(data, dict) and not data) or (isinstance(data, list) and not data) or \
            (not data):
        raise HTTPException(
            status_code=204, detail="Content is empty.")

    return data


@app.get("/protocols/{protocol_name}/cumulative-recent-pull-requests", dependencies=[Depends(get_current_user)],
         responses={
             200: {
                 "description": "Cumulative Recent Pull Requests",
                 "content": {
                     "application/json": {
                         "example": [{
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

                         }]
                     }
                 }
             },

             204: {
                 "description": "No content found.",
                 "content": {
                     "application/json": {
                         "example": None
                     }
                 }
             },

             404: {
                 "description": "Not found",
                 "content": {
                     "application/json": {
                         "example": {
                                "error": "Error description"
                         }
                     }
                 }
             },
}
)
def cumulative_recent_pull_requests(
    protocol_name: str = Path(..., description="Protocol name"),
    order_by: str = Query(..., description="Order by field"),
):
    """
    Returns the cumulative recent pull requests for the protocol.

    `order_by` can be one of the following values: CREATED_AT, UPDATED_AT

    """

    try:
        if order_by.strip().lower() not in ['created_at', 'updated_at']:
            raise HTTPException(
                status_code=400, detail="Invalid order by value")

        if order_by.strip().lower() == 'created_at':
            field_name = 'recent_created_pull_requests'

        elif order_by.strip().lower() == 'updated_at':
            field_name = 'recent_updated_pull_requests'

        ref = db.collection(f'{protocol_name}-cumulative').document(f'cumulative_{field_name}').get(
            field_paths=['data']).to_dict()

        if ref is None:
            raise exceptions.NotFound('Collection or document not found')

    except exceptions.NotFound as ex:
        # Handle case where document or collection does not exist
        raise HTTPException(
            status_code=404, detail=str(ex))

    except Exception as e:
        # Handle other exceptions
        raise HTTPException(
            status_code=404, detail=f"An error occurred {str(e)}")

    data = ref.get('data', None)
    if (data is None) or (isinstance(data, dict) and not data) or (isinstance(data, list) and not data) or \
            (not data):
        raise HTTPException(
            status_code=204, detail="Content is empty.")

    return data


@app.get("/protocols/{protocol_name}/cumulative-recent-commits", dependencies=[Depends(get_current_user)],
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
                 }
             },

             204: {
                 "description": "No content found.",
                 "content": {
                     "application/json": {
                         "example": None
                     }
                 }
             },

             404: {
                 "description": "Not found",
                 "content": {
                     "application/json": {
                         "example": {
                                "error": "Error description"
                         }
                     }
                 }
             },
}
)
def cumulative_recent_commits(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the cumulative recent commits for the protocol.

    """

    try:
        ref = db.collection(f'{protocol_name}-cumulative').document(f'cumulative_recent_commits').get(
            field_paths=['data']).to_dict()

        if ref is None:
            raise exceptions.NotFound('Collection or document not found')

    except exceptions.NotFound as ex:
        # Handle case where document or collection does not exist
        raise HTTPException(
            status_code=404, detail=str(ex))

    except Exception as e:
        # Handle other exceptions
        raise HTTPException(
            status_code=404, detail=f"An error occurred {str(e)}")

    data = ref.get('data', None)
    if (data is None) or (isinstance(data, dict) and not data) or (isinstance(data, list) and not data) or \
            (not data):
        raise HTTPException(
            status_code=204, detail="Content is empty.")

    return data


@app.get("/protocols/{protocol_name}/cumulative-recent-releases", dependencies=[Depends(get_current_user)],
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
                 }
             },

             204: {
                 "description": "No content found.",
                 "content": {
                     "application/json": {
                         "example": None
                     }
                 }
             },

             404: {
                 "description": "Not found",
                 "content": {
                     "application/json": {
                         "example": {
                                "error": "Error description"
                         }
                     }
                 }
             },
}
)
def cumulative_recent_releases(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the cumulative recent releases for the protocol.

    """

    try:
        ref = db.collection(f'{protocol_name}-cumulative').document(f'cumulative_recent_releases').get(
            field_paths=['data']).to_dict()

        if ref is None:
            raise exceptions.NotFound('Collection or document not found')

    except exceptions.NotFound as ex:
        # Handle case where document or collection does not exist
        raise HTTPException(
            status_code=404, detail=str(ex))

    except Exception as e:
        # Handle other exceptions
        raise HTTPException(
            status_code=404, detail=f"An error occurred {str(e)}")

    data = ref.get('data', None)
    if (data is None) or (isinstance(data, dict) and not data) or (isinstance(data, list) and not data) or \
            (not data):
        raise HTTPException(
            status_code=204, detail="Content is empty.")

    return data
