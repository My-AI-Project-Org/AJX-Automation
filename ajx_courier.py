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
        
        # 🔥 FIX 1: Use 'enumerate' to create a fresh sequence for the whole chapter
        for index, q in enumerate(data):
            # ✅ GLOBAL ID: Database ke liye unique key (e.g., 101, 102, 103...)
            q['id'] = current_global_id
            current_global_id += 1
            
            # ✅ LOCAL ID: Question Card ke liye sequential number (1, 2, 3...)
            # Ye ab image change hone par reset nahi hoga.
            new_sequence_num = index + 1
            q['local_id'] = new_sequence_num
            
            # Optional: Agar Android app 'display_num' use kar raha hai to usse bhi set karein
            q['display_num'] = new_sequence_num

            # Cleanup: Source image ka naam hata dein
            if 'source_image' in q: del q['source_image']
        
        end_id = current_global_id - 1
        payload_str = json.dumps(data)
        compressed = COMPRESSOR.compress(payload_str.encode('utf-8'))
        b64_payload = base64.b64encode(compressed).decode('utf-8')
        md5_hash = hashlib.md5(b64_payload.encode()).hexdigest()

        # 🔥 FIX 2: Python mein .trim() nahi hota, .strip() use karein
        safe_unit = unit_name.replace(".", "").replace("/", "_").upper().strip()
        safe_chap = chap_name.replace(".", "").replace("/", "_").upper().strip()
        ref_path = f"Syllabus/{subject_key}/Data/{safe_unit}/{safe_chap}"

        db.reference(ref_path).set({
            "status": "LIVE",
            "payload": b64_payload,
            "hash": md5_hash,
            "count": len(data),
            "id_range": f"{start_id}-{end_id}",
            "last_updated": int(time.time())
        })
        # Log message update kiya taaki clear ho ki local ID 1 se shuru ho rahi hai
        log("FIREBASE", f"🔥 Synced {chap_name} (Global IDs: {start_id}-{end_id} | Local IDs: 1-{len(data)})")
        return current_global_id

    def sync_chapter(self, subject_key, unit_name, chapter, current_global_id):
        chap_id = chapter['id']
        chap_name = chapter['chapter']
        
        # 🟢 PRE-SYNC AUDIT (Drive + Firebase)
        subject_folder_id = find_file_by_name(subject_key, parent_id=self.masonry_root_id)
        target_folder = self.get_chapter_folder(subject_folder_id, chap_id)
        if not target_folder: return current_global_id # Skip if folder missing

        # Chapter-wise Ledger Path in Firebase
        s_unit = unit_name.replace(".", "").replace("/", "_").upper().strip()
        s_chap = chap_name.replace(".", "").replace("/", "_").upper().strip()
        meta_ref = db.reference(f"Syllabus/{subject_key}/Metadata/{s_unit}/{s_chap}")
        
        # Drive Files Audit
        drive_files = {f['name'].upper(): f['id'] for f in list_files_in_folder(target_folder['id'])}
        has_data_json = "DATA.JSON" in drive_files
        
        # Firebase Metadata Audit
        chap_meta = meta_ref.get() or {}
        is_live = db.reference(f"Syllabus/{subject_key}/Data/{s_unit}/{s_chap}/status").get() == "LIVE"

        # --- 🔥 SCENARIO 1: SKIP LOGIC (Perfect Sync) ---
        if has_data_json and is_live:
            log("SKIP", f"⏭️ {chap_name} is already healthy and LIVE. Skipping.")
            return chap_meta.get("end_id", current_global_id - 1) + 1

        # --- 🔥 SCENARIO 2: REPAIR LOGIC (Drive File Missing but Range Reserved) ---
        if chap_meta.get("start_id"):
            log("WARNING", f"🛠️ Repairing {chap_name}. Re-using reserved Global IDs: {chap_meta['start_id']} onwards.")
            id_to_use = chap_meta["start_id"]
            is_new_chapter = False
        else:
            # --- 🔥 SCENARIO 3: NEW CHAPTER (Normal Flow) ---
            id_to_use = current_global_id
            is_new_chapter = True
            log("ACCOUNTANT", f"🆕 First time sync for {chap_name}. Starting Global ID: {id_to_use}")

        # --- CORE SYNC PROCESS ---
        merged_data = self.merge_oracle_jsons(drive_files) # Image 1.json, 2.json merge karega
        if not merged_data: return current_global_id

        # ID INJECTION (The Brains)
        for index, mcq in enumerate(merged_data):
            mcq['id'] = id_to_use + index       # Global Unique ID
            mcq['local_id'] = index + 1        # Card Sequence (1, 2, 3...)
            mcq['display_num'] = index + 1
            mcq['unit'] = unit_name.upper()
            mcq['chapter'] = chap_name.upper()

        # Update Accountant/Metadata
        new_end_id = id_to_use + len(merged_data) - 1
        
        # Save to Drive (Backup)
        upload_json(merged_data, "DATA.JSON", target_folder['id'])
        
        # Save to Firebase
        self.push_to_firebase(merged_data, subject_key, s_unit, s_chap, id_to_use, new_end_id)
        
        # Save Chapter Ledger (Range Reservation)
        meta_ref.update({
            "start_id": id_to_use,
            "end_id": new_end_id,
            "status": "LIVE",
            "count": len(merged_data)
        })

        # Agar naya chapter tha, toh Global Accountant badhao
        return (new_end_id + 1) if is_new_chapter else current_global_id

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
