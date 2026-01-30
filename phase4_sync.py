print("╔════════════════════════════════════════════════════╗")
print("║   AJX PHASE 4: FINAL SYNC & DISPATCH (COMPLETE)    ║")
print("╚════════════════════════════════════════════════════╝")

import os
import json
import shutil
import time
import sys
import urllib.request
import urllib.parse
from collections import deque

# Libs
import firebase_admin
from firebase_admin import credentials, db
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# --- CONFIG ---
# Phase 3 writes here, so Phase 4 reads from here
INPUT_ROOT = 'AJX_Worker_Output' 
BACKUP_DRIVE_FOLDER = 'AJX_Phase4_Backup'
DEFAULT_DB_URL = os.environ.get("FIREBASE_DB_URL") 

# --- 🟢 LIVE TELEGRAM TERMINAL SYSTEM ---
class TelegramTerminal:
    def __init__(self):
        self.token = os.environ.get("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.environ.get("TELEGRAM_CHAT_ID")
        self.message_id = None
        self.last_update_time = 0
        self.log_buffer = deque(maxlen=10) 
        self.current_progress = 0
        self.current_status = "Initializing Sync..."

    def start(self):
        if not self.token: return
        self.message_id = self._send_new("<b>🚀 AJX PHASE 4 (SYNC)</b>\nStarting Backup...")

    def log_stream(self, msg):
        clean_msg = str(msg).replace("<", "&lt;").replace(">", "&gt;") 
        self.log_buffer.append(f"> {clean_msg}")
        self._refresh_display()

    def update_progress(self, percent, status):
        self.current_progress = percent
        self.current_status = status
        self._refresh_display()

    def _refresh_display(self):
        if time.time() - self.last_update_time < 1.5 and self.current_progress < 100: return
        if not self.token or not self.message_id: return
        
        logs_text = "\n".join(self.log_buffer)
        bar_len = 10
        filled = int(bar_len * self.current_progress / 100)
        bar = "█" * filled + "░" * (bar_len - filled)
        
        text = (f"<b>🚀 AJX PHASE 4 (SYNC)</b>\n<code>{logs_text}</code>\n"
                f"━━━━━━━━━━━━━━━━━━\n<b>{self.current_status}</b>\n"
                f"<code>[{bar}] {self.current_progress}%</code>")
        self._edit_msg(text)
        self.last_update_time = time.time()

    def _send_new(self, text):
        try:
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            data = urllib.parse.urlencode({"chat_id": self.chat_id, "text": text, "parse_mode": "HTML"}).encode()
            with urllib.request.urlopen(urllib.request.Request(url, data=data)) as response:
                return json.loads(response.read())['result']['message_id']
        except: return None

    def _edit_msg(self, text):
        try:
            url = f"https://api.telegram.org/bot{self.token}/editMessageText"
            data = urllib.parse.urlencode({"chat_id": self.chat_id, "message_id": self.message_id, "text": text, "parse_mode": "HTML"}).encode()
            urllib.request.urlopen(urllib.request.Request(url, data=data))
        except: pass

terminal = TelegramTerminal()
def log(msg):
    print(msg)
    sys.stdout.flush()
    terminal.log_stream(msg)

# --- AUTH SETUP ---
firebase_key = os.environ.get("FIREBASE_SERVICE_ACCOUNT")
drive_key = os.environ.get("GDRIVE_OAUTH_JSON")

if not firebase_key or not drive_key:
    log("❌ Error: Secrets missing")
    exit(1)

# 1. Init Firebase
try:
    if not firebase_admin._apps:
        cred = credentials.Certificate(json.loads(firebase_key))
        db_url = DEFAULT_DB_URL if DEFAULT_DB_URL else "https://YOUR-APP.firebaseio.com/"
        firebase_admin.initialize_app(cred, {'databaseURL': db_url})
    log("✅ Firebase Connected")
except Exception as e:
    log(f"⚠️ Firebase Init Warning: {e}")

# 2. Init Drive
try:
    token_info = json.loads(drive_key)
    creds = Credentials.from_authorized_user_info(token_info, ['https://www.googleapis.com/auth/drive'])
    drive_service = build('drive', 'v3', credentials=creds)
    log("✅ Drive Connected")
except Exception as e:
    log(f"❌ Drive Auth Failed: {e}")
    exit(1)

# --- DRIVE FUNCTIONS ---
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
    # Check exists
    query = f"name = '{name}' and '{folder_id}' in parents and trashed = false"
    results = drive_service.files().list(q=query, fields="files(id)").execute()
    if len(results.get('files', [])) > 0: return 
        
    meta = {'name': name, 'parents': [folder_id]}
    media = MediaFileUpload(file_path, mimetype='application/json')
    drive_service.files().create(body=meta, media_body=media).execute()

def upload_zip_and_get_link(zip_path, folder_id):
    name = os.path.basename(zip_path)
    # 🧹 DELETE OLD FULL BOOK (Clean Overwrite)
    query = f"name = '{name}' and '{folder_id}' in parents and trashed = false"
    results = drive_service.files().list(q=query, fields="files(id)").execute()
    for f in results.get('files', []):
        try: drive_service.files().delete(fileId=f['id']).execute()
        except: pass

    # Upload New
    terminal.update_progress(70, "Uploading Full Book...")
    log(f"📦 Uploading Final Zip: {name}...")
    meta = {'name': name, 'parents': [folder_id]}
    media = MediaFileUpload(zip_path, mimetype='application/zip')
    file = drive_service.files().create(body=meta, media_body=media, fields='id').execute()
    
    # Permission Public
    drive_service.permissions().create(
        fileId=file['id'],
        body={'type': 'anyone', 'role': 'reader'}
    ).execute()
    
    return file['id']

# --- MAIN LOGIC ---
def main():
    terminal.start()
    
    if not os.path.exists(INPUT_ROOT):
        log(f"❌ Input Root missing: {INPUT_ROOT}")
        return
            
    # Try to find a meaningful name
    exam_folder_name = "AJX_Full_Book" 
    folders = [f for f in os.listdir(INPUT_ROOT) if os.path.isdir(os.path.join(INPUT_ROOT, f))]
    if folders:
        exam_folder_name = folders[0].split('_')[0] + "_Combined"
        
    log(f"🎯 Syncing Target: {exam_folder_name}")

    # 1. DRIVE BACKUP (Raw Files)
    terminal.update_progress(20, "Backing up Raw Files...")
    root_backup_id = get_or_create_folder(BACKUP_DRIVE_FOLDER)
    date_folder = f"Backup_{int(time.time())}"
    backup_id = get_or_create_folder(date_folder, root_backup_id)

    total_files = sum([len(files) for r, d, files in os.walk(INPUT_ROOT)])
    processed = 0

    for root, dirs, files in os.walk(INPUT_ROOT):
        for file in files:
            if file.endswith(".json"):
                rel_path = os.path.relpath(root, INPUT_ROOT)
                current_drive_id = backup_id
                
                if rel_path != ".":
                    for part in rel_path.split(os.sep):
                        current_drive_id = get_or_create_folder(part, current_drive_id)
                
                full_path = os.path.join(root, file)
                upload_file(full_path, current_drive_id)
                
                processed += 1
                if processed % 10 == 0:
                    percent = 20 + int((processed / total_files) * 30)
                    terminal.update_progress(percent, f"Saved: {file}")

    log("✅ Raw Backup Complete!")

    # 2. CREATE ZIP (FULL BOOK)
    terminal.update_progress(60, "Zipping Full Book...")
    log("🤐 Creating Package...")
    
    zip_name = "AJX_Full_Book_Update"
    shutil.make_archive(zip_name, 'zip', INPUT_ROOT)
    zip_path = zip_name + ".zip"

    # 3. UPLOAD ZIP (OVERWRITE OLD)
    try:
        zip_file_id = upload_zip_and_get_link(zip_path, root_backup_id)
        direct_link = f"https://drive.google.com/uc?export=download&id={zip_file_id}"
        log(f"✅ Full Book Link Generated.")
    except Exception as e:
        log(f"❌ Zip Upload Failed: {e}")
        return

    # 4. NOTIFY FIREBASE (FINAL UPDATE)
    terminal.update_progress(90, "Updating App...")
    log("🔔 Sending Final Notification...")
    
    try:
        ref = db.reference(f"updates/latest_book")
        update_data = {
            "version": int(time.time()),
            "zip_url": direct_link,
            "message": f"Full Book Update Available",
            "timestamp": str(time.ctime())
        }
        ref.set(update_data)
        log("✅ Firebase Updated! App will overwrite old data.")
    except Exception as e:
        log(f"❌ Firebase Error: {e}")

    terminal.update_progress(100, "✅ SYNC COMPLETE")
    log("🎉 All Systems Synced.")

if __name__ == "__main__":
    main()
