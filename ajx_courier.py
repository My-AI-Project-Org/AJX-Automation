import os
import json
import time
import io
import zstandard as zstd
import base64
import hashlib
import re
import sys
import firebase_admin
from firebase_admin import credentials, db
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# ==========================================
# ⚙️ CONFIGURATION
# ==========================================
SCOPES = ['https://www.googleapis.com/auth/drive']
COMPRESSOR = zstd.ZstdCompressor(level=3)

def log(level, msg):
    """Clean Logger"""
    icons = {
        "INFO": "ℹ️", "SUCCESS": "✅", "WARNING": "⚠️", 
        "ERROR": "❌", "COURIER": "🚚", "FIREBASE": "🔥", "MERGE": "🔗"
    }
    print(f"{icons.get(level, '')} [{level}] {msg}")

# ==========================================
# 🔐 AUTHENTICATION
# ==========================================
def setup_services():
    """Initializes Google Drive and Firebase Services"""
    drive_service = None
    
    # 1. Drive Auth
    try:
        if os.environ.get("GDRIVE_OAUTH_JSON"):
            creds = Credentials.from_authorized_user_info(json.loads(os.environ["GDRIVE_OAUTH_JSON"]), SCOPES)
            drive_service = build('drive', 'v3', credentials=creds)
        elif os.environ.get("GDRIVE_CREDENTIALS"):
            creds = service_account.Credentials.from_service_account_info(json.loads(os.environ["GDRIVE_CREDENTIALS"]), scopes=SCOPES)
            drive_service = build('drive', 'v3', credentials=creds)
        else:
            log("CRITICAL", "Missing Drive Credentials.")
            sys.exit(1)
    except Exception as e:
        log("CRITICAL", f"Drive Auth Failed: {e}")
        sys.exit(1)
    
    # 2. Firebase Auth
    try:
        if not firebase_admin._apps:
            key_json = os.environ.get("FIREBASE_SERVICE_KEY")
            if key_json:
                cred = credentials.Certificate(json.loads(key_json))
                firebase_admin.initialize_app(cred, {'databaseURL': os.environ.get("FIREBASE_DB_URL")})
            else:
                log("CRITICAL", "Missing FIREBASE_SERVICE_KEY.")
                sys.exit(1)
    except Exception as e:
        log("CRITICAL", f"Firebase Auth Failed: {e}")
        sys.exit(1)
    
    return drive_service

service = setup_services()

# ==========================================
# 📂 DRIVE HELPER FUNCTIONS
# ==========================================
def list_files_in_folder(folder_id):
    """Lists files to check for parts (1.json, 2.json...)"""
    try:
        query = f"'{folder_id}' in parents and trashed = false"
        results = service.files().list(q=query, fields="files(id, name)", pageSize=1000).execute()
        return results.get('files', [])
    except:
        return []

def find_file_by_name(name, parent_id=None):
    """Finds a folder/file ID"""
    try:
        query = f"name = '{name}' and trashed = false"
        if parent_id: query += f" and '{parent_id}' in parents"
        results = service.files().list(q=query, fields="files(id, name)").execute()
        files = results.get('files', [])
        return files[0]['id'] if files else None
    except:
        return None

def download_json(file_id):
    """Downloads and parses a JSON file from Drive"""
    try:
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done: status, done = downloader.next_chunk()
        fh.seek(0)
        return json.load(fh)
    except Exception as e:
        log("ERROR", f"Failed to download JSON {file_id}: {e}")
        return None

# ==========================================
# 🚚 THE COURIER ENGINE
# ==========================================
class AJXCourier:
    def __init__(self):
        self.bp_folder_id = find_file_by_name("01_Blueprints")
        self.masonry_root_id = find_file_by_name("02_Masonry")
        
        if not self.bp_folder_id or not self.masonry_root_id:
            log("CRITICAL", "Required Drive Folders (Blueprints/Masonry) not found.")
            sys.exit(1)

    def sync_chapter(self, subject_key, unit_name, chapter, current_global_id):
        """
        1. Checks Completeness (Audit)
        2. Merges Parts (1.json + 2.json...)
        3. Assigns Global IDs (The Backbone)
        4. Compresses & Pushes to Firebase
        """
        chap_id = chapter['id']
        chap_name = chapter['chapter']
        
        # 1. Find the Chapter Folder
        subject_folder_id = find_file_by_name(subject_key, parent_id=self.masonry_root_id)
        if not subject_folder_id: return None

        all_folders = list_files_in_folder(subject_folder_id)
        # Match folder starting with "ID_" (e.g., "101_Stone_Age")
        target_folder = next((f for f in all_folders if f['name'].startswith(f"{chap_id}_")), None)
        
        if not target_folder: return None # Folder doesn't exist yet

        # 2. AUDIT: Is it Complete?
        files = list_files_in_folder(target_folder['id'])
        file_map = {f['name']: f['id'] for f in files}
        
        if "meta.json" not in file_map: 
            return "STOP" # Not started yet

        meta = download_json(file_map["meta.json"])
        if not meta: return "STOP"
        
        total_parts_needed = meta.get('total_images', 0)
        
        # Count Valid JSON Parts (UPPERCASE or lowercase numbers)
        part_files = []
        for f in files:
            name = f['name']
            # Logic: Ends with .json AND is not meta/data AND (is a number or PART_X)
            if name.endswith('.json') and name not in ['meta.json', 'data.json']:
                part_files.append(f)
        
        # 🛑 STOPPING LOGIC: If parts are missing, we cannot proceed sequentially
        if len(part_files) < total_parts_needed:
            log("INFO", f"⏳ {chap_name} Incomplete ({len(part_files)}/{total_parts_needed}). Sequence Paused.")
            return "STOP"

        # 3. MERGE LOGIC
        # Sort files numerically (1.json, 2.json...)
        try:
            part_files.sort(key=lambda x: int(re.search(r'\d+', x['name']).group()))
        except: pass

        merged_data = []
        log("MERGE", f"🔗 Merging {len(part_files)} parts for {chap_name}...")
        
        for pf in part_files:
            data = download_json(pf['id'])
            if isinstance(data, list):
                merged_data.extend(data)
        
        if not merged_data: return None

        # 4. ASSIGN GLOBAL IDs (The Backbone) 🦴
        start_id = current_global_id
        
        for q in merged_data:
            q['id'] = current_global_id
            current_global_id += 1 # Increment for next question
            
            # Clean up temporary fields
            if 'local_id' in q: del q['local_id']
            if 'source_image' in q: del q['source_image']

        end_id = current_global_id - 1
        
        # 5. COMPRESS & UPLOAD TO FIREBASE
        payload_str = json.dumps(merged_data)
        compressed = COMPRESSOR.compress(payload_str.encode('utf-8'))
        b64_payload = base64.b64encode(compressed).decode('utf-8')
        md5_hash = hashlib.md5(b64_payload.encode()).hexdigest()

        # Firebase Path Sanitization
        safe_unit = unit_name.replace(".", "").replace("/", "_")
        safe_chap = chap_name.replace(".", "").replace("/", "_")
        
        ref_path = f"Syllabus/{subject_key}/Data/{safe_unit}/{safe_chap}"
        
        db.reference(ref_path).set({
            "status": "LIVE",
            "payload": b64_payload,
            "hash": md5_hash,
            "count": len(merged_data),
            "id_range": f"{start_id}-{end_id}",
            "last_updated": int(time.time())
        })

        log("SUCCESS", f"✅ Synced {chap_name} (Global IDs: {start_id}-{end_id})")
        
        return current_global_id # Return the New Counter

    def execute(self):
        log("INFO", "🚀 COURIER STARTED (Sequential Sync Mode)")
        
        blueprints = list_files_in_folder(self.bp_folder_id)
        
        for bp in blueprints:
            if not bp['name'].endswith("_BLUEPRINT.json"): continue
            
            # Load Blueprint
            bp_data = download_json(bp['id'])
            if not bp_data: continue
            
            subject_key = bp_data['meta']['subject_key']
            log("INFO", f"📜 Checking Subject: {subject_key}")
            
            

            # 1. Get Global ID Counter from Firebase Ledger
            config_ref = db.reference(f"Syllabus/{subject_key}/Config")
            config = config_ref.get() or {}
            
            # Start from 1001 if new, else continue from last point
            current_global_id = config.get('last_global_id', 1001) 
            
            stop_signal = False
            processed_something = False

            # 2. STRICT SEQUENTIAL LOOP
            # We iterate through the Blueprint order. 
            # If Chapter 1 is missing, we STOP. We do not touch Chapter 2.
            for unit in bp_data['structure']:
                if stop_signal: break
                for chap in unit['chapters']:
                    if stop_signal: break
                    
                    # Firebase Check: Is this already Live?
                    safe_unit = unit['unit_name'].replace(".", "").replace("/", "_")
                    safe_chap = chap['chapter'].replace(".", "").replace("/", "_")
                    status_ref = db.reference(f"Syllabus/{subject_key}/Data/{safe_unit}/{safe_chap}/status")
                    
                    if status_ref.get() == "LIVE":
                        # Already Done -> Skip processing
                        # NOTE: We assume the ID counter in Config is already correct 
                        # because we update it atomically after every sync.
                        continue 
                    
                    # Not Live -> Attempt to Sync
                    new_id = self.sync_chapter(subject_key, unit['unit_name'], chap, current_global_id)
                    
                    if new_id == "STOP":
                        stop_signal = True # 🛑 Stop sequence to prevent ID gaps
                        log("INFO", f"🛑 Sequence stopped at {chap['chapter']}. Waiting for Oracle.")
                    elif new_id and isinstance(new_id, int):
                        # Success! Update Global Counter immediately
                        current_global_id = new_id
                        config_ref.update({"last_global_id": current_global_id})
                        processed_something = True
            
            if processed_something:
                log("INFO", f"🎉 New Content Live! Global ID Counter is now: {current_global_id}")
            else:
                log("INFO", "💤 No new complete chapters to sync.")

if __name__ == "__main__":
    AJXCourier().execute()
