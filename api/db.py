import os
import firebase_admin
from firebase_admin import firestore, credentials
import os
import tools.log_config as log_config

# Get the current file's directory
current_dir = os.path.dirname(os.path.realpath(__file__))

# Go up one level to the parent directory (which is the main project directory)
project_dir = os.path.dirname(current_dir)

# Construct the path to the admin_sdk.json file
admin_sdk_path = os.path.join(project_dir, os.environ["FIREBASE_ADMIN_SDK_NAME"])

if not os.path.exists(admin_sdk_path):
    raise Exception(f"Admin SDK file not found in path {admin_sdk_path}")

cred = credentials.Certificate(admin_sdk_path)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = admin_sdk_path

app = firebase_admin.initialize_app(cred, {"projectId": os.environ["FIREBASE_PROJECT_ID"]}, name="flowana_api")

db = firestore.Client()
