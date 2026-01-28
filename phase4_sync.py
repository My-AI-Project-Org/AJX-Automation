import os
import json
import shutil
import time
import firebase_admin
from firebase_admin import credentials, db
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# --- CONFIG ---
INPUT_ROOT = 'AJX_OUTPUT_Phase2' # Workers ne yahan maal rakha hai
BACKUP_DRIVE_FOLDER = 'AJX_Phase4_Backup'

# --- AUTH SETUP (Drive & Firebase) ---
firebase_key = os.environ.get("FIREBASE_SERVICE_KEY")
drive_key = os.environ.get("GDRIVE_OAUTH_JSON")

if not firebase_key or not drive_key:
    print("❌ Error: Secrets missing (FIREBASE_SERVICE_KEY or GDRIVE_OAUTH_JSON)")
    exit(1)

# 1. Init Firebase
cred = credentials.Certificate(json.loads(firebase_key))
# 👇👇 APNA DATABASE URL YAHAN REPLACE KAREIN 👇👇
DATABASE_URL = 'https://console.firebase.google.com/u/1/project/ajx-mcq-app-f5ba1/database/ajx-mcq-app-f5ba1-default-rtdb/data/~2F' 
try:
    firebase_admin.initialize_app(cred, {'databaseURL': DATABASE_URL})
except: pass # Already initialized

# 2. Init Drive
token_info = json.loads(drive_key)
creds = Credentials.from_authorized_user_info(token_info, ['https://www.googleapis.com/auth/drive'])
drive_service = build('drive', 'v3', credentials=creds)

# --- DRIVE HELPER FUNCTIONS ---
def get_or_create_folder(folder_name, parent_id=None):
    query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    if parent_id: query += f" and '{parent_id}' in parents"
    results = drive_service.files().list(q=query, fields="files(id)").execute()
    files = results.get('files', [])
    
    if files: return files[0]['id']
    
    meta = {'name': folder_name, 'mimeType': 'application/vnd.google-apps.folder'}
    if parent_id: meta['parents'] = [parent_id]
    folder = drive_service.files().create(body=meta, fields='id').execute()
    return folder.get('id')

def upload_file(file_path, folder_id):
    name = os.path.basename(file_path)
    meta = {'name': name, 'parents': [folder_id]}
    media = MediaFileUpload(file_path, mimetype='application/json')
    drive_service.files().create(body=meta, media_body=media).execute()

def upload_zip_and_get_link(zip_path, folder_id):
    name = os.path.basename(zip_path)
    # Check if exists, delete old
    query = f"name = '{name}' and '{folder_id}' in parents and trashed = false"
    results = drive_service.files().list(q=query, fields="files(id)").execute()
    for f in results.get('files', []):
        drive_service.files().delete(fileId=f['id']).execute()

    # Upload New
    print(f"📦 Uploading Zip: {name}...")
    meta = {'name': name, 'parents': [folder_id]}
    media = MediaFileUpload(zip_path, mimetype='application/zip')
    file = drive_service.files().create(body=meta, media_body=media, fields='id, webViewLink, webContentLink').execute()
    
    # Permission Public (Taaki App download kar sake)
    drive_service.permissions().create(
        fileId=file['id'],
        body={'type': 'anyone', 'role': 'reader'}
    ).execute()
    
    # Direct Download Link (Expert Trick)
    return file['id'] # We will construct direct link in App

# --- MAIN LOGIC ---
print("🚀 PHASE 4 STARTED: Backup & Dispatch...")

# Exam Name detect karna (Folder structure se)
# Structure: AJX_OUTPUT_Phase2 / UPSI_History / Unit1...
try:
    exam_folder_name = os.listdir(INPUT_ROOT)[0] # e.g., "UPSI_History"
except:
    print("❌ No Output found.")
    exit()

print(f"   Target: {exam_folder_name}")

# 1. DRIVE BACKUP (Raw Files)
print("☁️ Starting Raw Backup to Drive...")
root_backup_id = get_or_create_folder(BACKUP_DRIVE_FOLDER)
exam_backup_id = get_or_create_folder(exam_folder_name, root_backup_id)

for root, dirs, files in os.walk(os.path.join(INPUT_ROOT, exam_folder_name)):
    for file in files:
        if file.endswith(".json"):
            # Recreate hierarchy
            rel_path = os.path.relpath(root, os.path.join(INPUT_ROOT, exam_folder_name))
            
            # Navigate/Create folders in Drive
            current_drive_id = exam_backup_id
            if rel_path != ".":
                for part in rel_path.split(os.sep):
                    current_drive_id = get_or_create_folder(part, current_drive_id)
            
            # Upload File
            full_path = os.path.join(root, file)
            upload_file(full_path, current_drive_id)

print("✅ Raw Backup Complete!")

# 2. CREATE ZIP
print("🤐 Creating Zip Package...")
zip_filename = f"{exam_folder_name}_update.zip"
shutil.make_archive(exam_folder_name, 'zip', INPUT_ROOT) # Creates .zip locally
zip_path = exam_folder_name + ".zip"

# 3. UPLOAD ZIP
zip_file_id = upload_zip_and_get_link(zip_path, root_backup_id)
# Direct Link Format for Android
direct_link = f"https://drive.google.com/uc?export=download&id={zip_file_id}"

print(f"✅ Zip Uploaded! ID: {zip_file_id}")

# 4. NOTIFY FIREBASE
print("🔔 Sending Notification to App...")
ref = db.reference(f"updates/{exam_folder_name}")

update_data = {
    "version": int(time.time()), # Unique Timestamp
    "zip_url": direct_link,
    "message": f"New content added for {exam_folder_name}",
    "timestamp": str(time.ctime())
}

ref.set(update_data)
print("✅ Firebase Updated! App will now Sync.")
