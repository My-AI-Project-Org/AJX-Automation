import os
import json
import firebase_admin
from firebase_admin import credentials, db

# --- CONFIG ---
INPUT_ROOT = 'AJX_OUTPUT_Phase2'

# Check for Key
key_json = os.environ.get("FIREBASE_SERVICE_KEY")
if not key_json:
    print("⚠️ Skipping Phase 4: FIREBASE_SERVICE_KEY not found in Secrets.")
    exit(0)

# Initialize Firebase
cred = credentials.Certificate(json.loads(key_json))

# 👇👇 APNA DATABASE URL YAHAN REPLACE KAREIN 👇👇
# Example: 'https://ajx-mcq-app-default-rtdb.firebaseio.com/'
DATABASE_URL = 'https://console.firebase.google.com/u/1/project/ajx-mcq-app-f5ba1/database/ajx-mcq-app-f5ba1-default-rtdb/data/~2F' 

try:
    firebase_admin.initialize_app(cred, {'databaseURL': DATABASE_URL})
except ValueError:
    print("❌ Error: Invalid Database URL. Please update phase4_sync.py")
    exit(1)

ref = db.reference('exams')

print("🚀 PHASE 4 STARTED: Syncing to Android App...")

# Walk through folders
for root, _, files in os.walk(INPUT_ROOT):
    for file in files:
        if file.endswith(".json"):
            # Path Construction
            # Local: AJX_OUTPUT/UPSI_History/Unit1/Chapter1/1.json
            # Cloud: UPSI_History/Unit1/Chapter1/Page_1

            rel_path = os.path.relpath(root, INPUT_ROOT)
            # Ensure forward slashes for URL
            path_parts = rel_path.replace("\\", "/").split("/")

            # File name becomes Page node
            page_node = "Page_" + os.path.splitext(file)[0]

            # Full Path
            db_path = "/".join(path_parts) + "/" + page_node

            # Load Data
            file_full_path = os.path.join(root, file)
            with open(file_full_path, 'r', encoding='utf-8') as f:
                mcq_data = json.load(f)

            # Inject
            print(f"   ☁️ Uploading to: {db_path}")
            try:
                ref.child(db_path).set(mcq_data)
            except Exception as e:
                print(f"   ❌ Failed to upload {file}: {e}")

print("✅ SYNC COMPLETE! Data is live on App.")
