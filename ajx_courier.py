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
    print(f"{icons.get(level, '')} [{level}] {msg}",flush=True)

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
# 🚚 THE COURIER ENGINE (FIXED & COMPLETE)
# ==========================================
class AJXCourier:
    def __init__(self):
        self.bp_folder_id = find_file_by_name("01_Blueprints")
        self.masonry_root_id = find_file_by_name("02_Masonry")
        if not self.bp_folder_id or not self.masonry_root_id:
            log("CRITICAL", "Required Drive Folders not found.")
            sys.exit(1)

    # 🛠️ HELPER 1: Merge Oracle Files (Missing in your script)
    def merge_oracle_jsons(self, drive_files_map):
        part_files = [
            {'name': name, 'id': fid} 
            for name, fid in drive_files_map.items() 
            if name.endswith('.JSON') and name not in ['META.JSON', 'DATA.JSON']
        ]
        # Sort by number (1.json, 2.json...)
        part_files.sort(key=lambda x: int(re.search(r'\d+', x['name']).group()) if re.search(r'\d+', x['name']) else 0)
        
        merged = []
        for pf in part_files:
            try:
                data = download_json(pf['id'])
                if isinstance(data, list): merged.extend(data)
            except Exception as e:
                log("WARNING", f"Failed to merge {pf['name']}: {e}")
        return merged

    # 🛠️ HELPER 2: Get Chapter Folder (Missing in your script)
    def get_chapter_folder(self, subject_folder_id, chap_id):
        all_folders = list_files_in_folder(subject_folder_id)
        return next((f for f in all_folders if f['name'].startswith(f"{chap_id}_")), None)

    # 🛠️ CORE: Push to Firebase (Updated: No ID Logic here, just Push)
    def push_to_firebase(self, data, subject_key, s_unit, s_chap, start_id, end_id):
        payload_str = json.dumps(data)
        compressed = COMPRESSOR.compress(payload_str.encode('utf-8'))
        b64_payload = base64.b64encode(compressed).decode('utf-8')
        md5_hash = hashlib.md5(b64_payload.encode()).hexdigest()

        ref_path = f"Syllabus/{subject_key}/Data/{s_unit}/{s_chap}"
        
        db.reference(ref_path).set({
            "status": "LIVE",
            "payload": b64_payload,
            "hash": md5_hash,
            "count": len(data),
            "id_range": f"{start_id}-{end_id}",
            "last_updated": int(time.time())
        })
        log("FIREBASE", f"🔥 Synced {s_chap} (Global IDs: {start_id}-{end_id})")

    # 🛠️ CORE: Sync Logic (The Brain - Reordered)
    def sync_chapter(self, subject_key, unit_name, chapter, current_global_id):
        chap_id = chapter['id']
        chap_name = chapter['chapter']
        
        # 1. Access Folder
        subject_folder_id = find_file_by_name(subject_key, parent_id=self.masonry_root_id)
        target_folder = self.get_chapter_folder(subject_folder_id, chap_id)
        if not target_folder: 
            return current_global_id

        # 2. Setup Paths
        s_unit = unit_name.replace(".", "").replace("/", "_").upper().strip()
        s_chap = chap_name.replace(".", "").replace("/", "_").upper().strip()
        meta_ref = db.reference(f"Syllabus/{subject_key}/Metadata/{s_unit}/{s_chap}")
        
        # 3. Audit Files
        drive_files_list = list_files_in_folder(target_folder['id'])
        drive_files_map = {f['name'].upper(): f['id'] for f in drive_files_list}
        has_data_json = "DATA.JSON" in drive_files_map
        
        # 4. Check Skip Logic
        chap_meta = meta_ref.get() or {}
        is_live = db.reference(f"Syllabus/{subject_key}/Data/{s_unit}/{s_chap}/status").get() == "LIVE"

        # Scenario 1: Everything Good -> Skip
        if has_data_json and is_live:
            log("SKIP", f"⏭️ {chap_name} is already LIVE. Skipping.")
            return chap_meta.get("end_id", current_global_id - 1) + 1

        # Scenario 2: Repair (Use Old Range)
        if chap_meta.get("start_id"):
            id_to_use = chap_meta["start_id"]
            log("ACCOUNTANT", f"🛠️ Repairing {chap_name} using Reserved IDs: {id_to_use}...")
            is_new_chapter = False
        else:
            # Scenario 3: New Chapter
            id_to_use = current_global_id
            log("ACCOUNTANT", f"🆕 Syncing New Chapter {chap_name} starting from {id_to_use}...")
            is_new_chapter = True

        # 5. Merge Files
        merged_data = self.merge_oracle_jsons(drive_files_map)
        if not merged_data: return current_global_id

        # 6. INJECT IDs (Data fixed IN MEMORY first)
        for index, mcq in enumerate(merged_data):
            mcq['id'] = id_to_use + index      # Global ID (101, 102...)
            mcq['local_id'] = index + 1        # Local Sequence (1, 2, 3...)
            mcq['display_num'] = index + 1
            mcq['unit'] = unit_name.upper()
            mcq['chapter'] = chap_name.upper()
            if 'source_image' in mcq: del mcq['source_image']

        new_end_id = id_to_use + len(merged_data) - 1

        # 7. Save & Push (Correct Order)
        # A. Save Fixed Data to Drive (Now Drive has correct IDs)
        upload_json(merged_data, "DATA.JSON", target_folder['id'])
        
        # B. Push to Firebase
        self.push_to_firebase(merged_data, subject_key, s_unit, s_chap, id_to_use, new_end_id)
        
        # C. Update Ledger
        meta_ref.update({
            "start_id": id_to_use,
            "end_id": new_end_id,
            "status": "LIVE",
            "count": len(merged_data)
        })

        return (new_end_id + 1) if is_new_chapter else current_global_id

    def execute(self):
        log("INFO", "🚀 COURIER STARTED (Self-Healing Mode)")
        
        ledger_ref = db.reference("App_Settings/Accountant")
        last_global_id = (ledger_ref.get() or {}).get('last_used_id', 100)

        blueprints = list_files_in_folder(self.bp_folder_id)
        for bp in blueprints:
            if not bp['name'].endswith("_BLUEPRINT.json"): continue
            
            bp_data = download_json(bp['id'])
            subject_key = bp_data['meta']['subject_key']
            
            running_global_id = last_global_id + 1
            max_id_reached = last_global_id

            for unit in bp_data['structure']:
                for chap in unit['chapters']:
                    # Sync Chapter
                    next_id = self.sync_chapter(subject_key, unit['unit_name'], chap, running_global_id)
                    
                    # Update running ID
                    running_global_id = next_id
                    if next_id > max_id_reached: max_id_reached = next_id - 1

            # Update Global Accountant
            if max_id_reached > last_global_id:
                ledger_ref.update({"last_used_id": max_id_reached})
                log("SUCCESS", f"🎉 Accountant Updated. New High Watermark: {max_id_reached}")

if __name__ == "__main__":
    AJXCourier().execute()
