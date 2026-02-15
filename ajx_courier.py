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
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

# ==========================================
# ⚙️ CONFIGURATION
# ==========================================
SCOPES = ['https://www.googleapis.com/auth/drive']
COMPRESSOR = zstd.ZstdCompressor(level=3)

def log(level, msg):
    """Professional Status Logger"""
    icons = {
        "INFO": "ℹ️", "SUCCESS": "✅", "WARNING": "⚠️", 
        "ERROR": "❌", "COURIER": "🚚", "FIREBASE": "🔥", "MERGE": "🔗", "ACCOUNTANT": "💰"
    }
    print(f"{icons.get(level, '')} [{level}] {msg}")

# ==========================================
# 🔐 AUTHENTICATION
# ==========================================
def setup_services():
    drive_service = None
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
    query = f"'{folder_id}' in parents and trashed = false"
    results = service.files().list(q=query, fields="files(id, name)", pageSize=1000).execute()
    return results.get('files', [])

def find_file_by_name(name, parent_id=None):
    query = f"name = '{name}' and trashed = false"
    if parent_id: query += f" and '{parent_id}' in parents"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get('files', [])
    return files[0]['id'] if files else None

def download_json(file_id):
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done: _, done = downloader.next_chunk()
    fh.seek(0)
    return json.load(fh)

def upload_json(data, filename, folder_id):
    local_path = f"temp_{filename}"
    with open(local_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)
    meta = {'name': filename, 'parents': [folder_id]}
    media = MediaFileUpload(local_path, mimetype='application/json')
    service.files().create(body=meta, media_body=media).execute()
    if os.path.exists(local_path): os.remove(local_path)

# ==========================================
# 🚚 THE COURIER ENGINE
# ==========================================
class AJXCourier:
    def __init__(self):
        self.bp_folder_id = find_file_by_name("01_Blueprints")
        self.masonry_root_id = find_file_by_name("02_Masonry")
        if not self.bp_folder_id or not self.masonry_root_id:
            log("CRITICAL", "Required Drive Folders not found.")
            sys.exit(1)

    def push_to_firebase(self, data, subject_key, unit_name, chap_name, current_global_id):
        """Standardized Push: Assign IDs -> Compress -> Base64 -> Firebase"""
        start_id = current_global_id
        for q in data:
            q['id'] = current_global_id
            current_global_id += 1
            # Clean up temp fields for clean App JSON
            if 'local_id' in q: q['display_num'] = q.pop('local_id')
            if 'source_image' in q: del q['source_image']
        
        end_id = current_global_id - 1
        payload_str = json.dumps(data)
        compressed = COMPRESSOR.compress(payload_str.encode('utf-8'))
        b64_payload = base64.b64encode(compressed).decode('utf-8')
        md5_hash = hashlib.md5(b64_payload.encode()).hexdigest()

        # Android Path: Syllabus/Subject/Data/Unit/Chapter
        safe_unit = unit_name.replace(".", "").replace("/", "_").upper().trim()
        safe_chap = chap_name.replace(".", "").replace("/", "_").upper().trim()
        ref_path = f"Syllabus/{subject_key}/Data/{safe_unit}/{safe_chap}"

        db.reference(ref_path).set({
            "status": "LIVE",
            "payload": b64_payload,
            "hash": md5_hash,
            "count": len(data),
            "id_range": f"{start_id}-{end_id}",
            "last_updated": int(time.time())
        })
        log("FIREBASE", f"🔥 Synced {chap_name} (IDs: {start_id}-{end_id})")
        return current_global_id

    def sync_chapter(self, subject_key, unit_name, chapter, current_global_id):
        chap_id = chapter['id']
        chap_name = chapter['chapter']
        
        # 1. Access Chapter Folder on Drive
        subject_folder_id = find_file_by_name(subject_key, parent_id=self.masonry_root_id)
        all_folders = list_files_in_folder(subject_folder_id)
        target_folder = next((f for f in all_folders if f['name'].startswith(f"{chap_id}_")), None)
        if not target_folder: return None

        # 2. Double-Check Drive vs Firebase (Self-Healing)
        files = list_files_in_folder(target_folder['id'])
        file_map = {f['name'].upper(): f['id'] for f in files}
        
        safe_unit = unit_name.replace(".", "").replace("/", "_").upper().strip()
        safe_chap = chap_name.replace(".", "").replace("/", "_").upper().strip()
        fb_status = db.reference(f"Syllabus/{subject_key}/Data/{safe_unit}/{safe_chap}/status").get()

        if "DATA.JSON" in file_map and fb_status != "LIVE":
            log("INFO", f"♻️ Healing: Found DATA.JSON on Drive for {chap_name}. Restoring...")
            merged_data = download_json(file_map["DATA.JSON"])
            return self.push_to_firebase(merged_data, subject_key, unit_name, chap_name, current_global_id)

        # 3. Perform Normal Audit & Merge
        if "META.JSON" not in file_map and "meta.json" not in file_map: return "STOP"
        
        meta_id = file_map.get("META.JSON") or file_map.get("meta.json")
        meta = download_json(meta_id)
        total_parts_needed = meta.get('total_images', 0)
        
        part_files = [f for f in files if f['name'].upper().endswith('.JSON') and f['name'].upper() not in ['META.JSON', 'DATA.JSON']]
        
        if len(part_files) < total_parts_needed:
            log("INFO", f"⏳ {chap_name} Incomplete ({len(part_files)}/{total_parts_needed}). Paused.")
            return "STOP"

        # 4. Merge Logic
        part_files.sort(key=lambda x: int(re.search(r'\d+', x['name']).group()) if re.search(r'\d+', x['name']) else 0)
        merged_data = []
        for pf in part_files:
            data = download_json(pf['id'])
            if isinstance(data, list): merged_data.extend(data)

        # Save Final DATA.JSON to Drive for Backup
        upload_json(merged_data, "DATA.JSON", target_folder['id'])
        log("SUCCESS", f"💾 DATA.JSON Saved for {chap_name}.")

        return self.push_to_firebase(merged_data, subject_key, unit_name, chap_name, current_global_id)

    def execute(self):
        log("INFO", "🚀 COURIER STARTED (Sequential Mode)")
        
        # 🟢 GLOBAL ACCOUNTANT (Starts from 101)
        ledger_ref = db.reference("App_Settings/Accountant")
        last_used_id = (ledger_ref.get() or {}).get('last_used_id', 100)
        current_global_id = last_used_id + 1
        log("ACCOUNTANT", f"💰 Starting from Global ID: {current_global_id}")

        blueprints = list_files_in_folder(self.bp_folder_id)
        for bp in blueprints:
            if not bp['name'].endswith("_BLUEPRINT.json"): continue
            
            bp_data = download_json(bp['id'])
            subject_key = bp_data['meta']['subject_key']
            
            stop_signal = False
            processed_any = False

            # Strict Sequential Loop
            for unit in bp_data['structure']:
                if stop_signal: break
                for chap in unit['chapters']:
                    if stop_signal: break
                    
                    s_unit = unit['unit_name'].replace(".", "").replace("/", "_").upper().strip()
                    s_chap = chap['chapter'].replace(".", "").replace("/", "_").upper().strip()
                    status = db.reference(f"Syllabus/{subject_key}/Data/{s_unit}/{s_chap}/status").get()
                    
                    if status == "LIVE": continue
                    
                    new_id = self.sync_chapter(subject_key, unit['unit_name'], chap, current_global_id)
                    
                    if new_id == "STOP":
                        stop_signal = True
                    elif isinstance(new_id, int):
                        current_global_id = new_id
                        processed_any = True
                        # Update Accountant immediately for safety
                        ledger_ref.update({"last_used_id": current_global_id - 1})

            if processed_any:
                log("SUCCESS", f"🎉 {subject_key} Synced! Last ID: {current_global_id - 1}")

if __name__ == "__main__":
    AJXCourier().execute()
