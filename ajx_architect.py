import os
import json
import time
import re
import io
import sys
import google.generativeai as genai
import firebase_admin
from firebase_admin import credentials, db
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

# ==========================================
# ⚙️ CONFIGURATION (PLATINUM LOGIC)
# ==========================================

# 🔴 HARDCODED DRIVE ROOT ID (Your AJX_Factory Folder)
DRIVE_ROOT_ID = "1i_YALAikZVwKmSlor6QgF5dm-0hhMxSw"

SCOPES = ['https://www.googleapis.com/auth/drive']

def log(level, msg):
    """Custom Logger"""
    icons = {"INFO": "ℹ️", "SUCCESS": "✅", "WARNING": "⚠️", "ERROR": "❌", "CRITICAL": "💀", "DSA": "🧮"}
    print(f"{icons.get(level, '')} [{level}] {msg}")

# ==========================================
# 🔐 AUTHENTICATION (HUMAN PRIORITY)
# ==========================================
def setup_auth():
    """
    Robust Auth Logic.
    PRIORITY 1: Human OAuth (Full Storage)
    PRIORITY 2: Service Account (Zero Storage - Fallback)
    """
    log("INFO", "Initializing Auth Logic...")

    # 1. Setup Gemini Key
    keys_json = os.environ.get("GEMINI_API_KEYS_LIST")
    if keys_json:
        try:
            if "[" in keys_json:
                keys = json.loads(keys_json)
                key = keys[0]
            else:
                key = keys_json.split(',')[0].strip()
            
            clean_key = key.replace('"', '').replace("'", "").strip()
            genai.configure(api_key=clean_key)
            log("SUCCESS", "Gemini API Configured.")
        except Exception as e:
            log("CRITICAL", f"Gemini Key Error: {e}")
            sys.exit(1)
    else:
        HARDCODED_KEY = "AIzaSyDmb1hHM0Qn_BKllH0Ev9xVU1EG8k6_53c"
        genai.configure(api_key=HARDCODED_KEY)
        log("WARNING", "Using Hardcoded Gemini Key.")

    # 2. Setup Drive Auth - TRY HUMAN FIRST (OAuth)
    oauth_json = os.environ.get("GDRIVE_OAUTH_JSON")
    if oauth_json:
        try:
            log("INFO", "Attempting Human Auth (OAuth)...")
            token_info = json.loads(oauth_json)
            creds = Credentials.from_authorized_user_info(token_info, SCOPES)
            service = build('drive', 'v3', credentials=creds)
            log("SUCCESS", "✅ Authenticated as HUMAN (Full Storage Access).")
            return service
        except Exception as e:
            log("WARNING", f"Human Auth Failed: {e}. Falling back to Robot...")

    # 3. Setup Drive Auth - FALLBACK TO ROBOT (Service Account)
    sa_json = os.environ.get("GDRIVE_CREDENTIALS")
    if sa_json:
        try:
            log("INFO", "Attempting Robot Auth (Service Account)...")
            # Load directly from string (no file write needed)
            creds = service_account.Credentials.from_service_account_info(
                json.loads(sa_json), scopes=SCOPES)
            service = build('drive', 'v3', credentials=creds)
            log("SUCCESS", "🤖 Authenticated as ROBOT (Zero Storage Quota).")
            return service
        except Exception as e:
            log("WARNING", f"Service Account Auth Failed: {e}")

    log("CRITICAL", "❌ FATAL: No valid Drive Auth found. Please set GDRIVE_OAUTH_JSON.")
    sys.exit(1)

# Initialize Service globally
service = setup_auth()

# --- FIREBASE INIT ---
if not firebase_admin._apps:
    try:
        # Try to use the same service account file if valid
        if os.path.exists("credentials.json"):
            cred = credentials.Certificate("credentials.json")
        else:
            # Fallback to separate env var
            fb_key = os.environ.get("FIREBASE_SERVICE_KEY")
            if fb_key:
                # Use a temp file for Firebase cert
                with open("firebase_key.json", "w") as f: f.write(fb_key)
                cred = credentials.Certificate("firebase_key.json")
            else:
                raise Exception("No Firebase Key found")
                
        firebase_admin.initialize_app(cred, {'databaseURL': os.environ.get("FIREBASE_DB_URL")})
        log("SUCCESS", "Firebase Connected.")
    except Exception as e:
        log("ERROR", f"Firebase Connection Failed (Skipping DB Sync): {e}")

# ==========================================
# 📂 FOLDER LOGIC (COPIED FROM PLATINUM SCRIPT)
# ==========================================

def get_folder_id(folder_name, parent_id=DRIVE_ROOT_ID):
    """Finds a folder specifically inside the parent_id."""
    if not parent_id: parent_id = DRIVE_ROOT_ID
    
    clean_parent = parent_id.strip().split('/')[-1]
    
    query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false and '{clean_parent}' in parents"
    results = service.files().list(q=query, fields="files(id)").execute()
    files = results.get('files', [])
    return files[0]['id'] if files else None

def create_folder(folder_name, parent_id=DRIVE_ROOT_ID):
    """Creates a folder if it doesn't exist."""
    if not parent_id: parent_id = DRIVE_ROOT_ID
    
    existing = get_folder_id(folder_name, parent_id)
    if existing: return existing
    
    log("INFO", f"Creating missing folder: {folder_name}")
    meta = {
        'name': folder_name, 
        'mimeType': 'application/vnd.google-apps.folder',
        'parents': [parent_id]
    }
    folder = service.files().create(body=meta, fields='id').execute()
    return folder.get('id')

def list_files_in_folder(folder_id):
    """Lists files inside a folder."""
    query = f"'{folder_id}' in parents and trashed = false"
    results = service.files().list(q=query, fields="files(id, name, mimeType)").execute()
    return results.get('files', [])

def download_file(file_id, local_name):
    try:
        request = service.files().get_media(fileId=file_id)
        fh = io.FileIO(local_name, 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        print(f"⬇️ Downloading {local_name}", end="")
        while not done:
            status, done = downloader.next_chunk()
            print(".", end="")
        print(" Done!")
        fh.close()
        return True
    except Exception as e:
        log("ERROR", f"Download Failed ({local_name}): {e}")
        return False

def upload_json(data, filename, folder_id):
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)
        
        meta = {'name': filename, 'parents': [folder_id]}
        media = MediaFileUpload(filename, mimetype='application/json')
        service.files().create(body=meta, media_body=media).execute()
        log("SUCCESS", f"Blueprint Uploaded: {filename}")
        
        if os.path.exists(filename): os.remove(filename)
    except Exception as e:
        log("ERROR", f"Upload Failed ({filename}): {e}")

# ==========================================
# 🏛️ THE ARCHITECT (Brain of the Operation)
# ==========================================
class AJXArchitect:
    def __init__(self):
        log("INFO", "Architect initializing workspace...")
        
        # 1. Ensure 01_Blueprints exists inside Root
        self.blueprint_id = create_folder("01_Blueprints", DRIVE_ROOT_ID)
        log("SUCCESS", f"Blueprints Folder Ready: {self.blueprint_id}")
        
        # 2. Ensure 00_Input exists inside Root
        self.input_id = create_folder("00_Input", DRIVE_ROOT_ID)
        log("SUCCESS", f"Input Folder Ready: {self.input_id}")

    def clean_filename(self, text):
        """Sanitizes strings AND Converts to UPPERCASE"""
        text = text.upper()
        text = text.replace(" ", "_")
        return re.sub(r'[^A-Z0-9_]', '', text)

    def analyze_index_method_1(self, index_path):
        """METHOD 1: Gemini PDF Analysis"""
        log("INFO", "Method 1 Detected: Starting AI Index Analysis...")
        try:
            file = genai.upload_file(index_path, display_name="Index PDF")
            while file.state.name == "PROCESSING":
                time.sleep(2)
                file = genai.get_file(file.name)
            
            if file.state.name == "FAILED": raise Exception("Gemini File State: FAILED")

            prompt = """
            Act as a Syllabus Architect. Parse this Index PDF.
            Output a strict JSON structure:
            [
              {
                "unit_name": "Unit Name",
                "chapters": [
                  { "chapter": "Chapter Name", "start_p": 10, "end_p": 20 }
                ]
              }
            ]
            """
            model = genai.GenerativeModel("gemini-2.0-flash")
            response = model.generate_content([file, prompt])
            text = response.text.replace("```json", "").replace("```", "").strip()
            return json.loads(text)
        except Exception as e:
            log("ERROR", f"AI Analysis Failed: {e}")
            return []

    def process_db_method_2(self, db_data):
        """METHOD 2: Database Conversion"""
        log("INFO", "Method 2 Detected: Converting DB to Skeleton...")
        grouped = {}
        for item in db_data:
            unit_key = item.get('chapter', 'General Module')
            topic_name = item.get('topic', 'General Topic')
            
            if unit_key not in grouped: grouped[unit_key] = []
            
            grouped[unit_key].append({
                "chapter": topic_name,
                "method_2_meta": {
                    "subtopic": item.get('subtopic', ''),
                    "num_questions": item.get('num_questions', 5),
                    "format": item.get('format', 'Standard')
                }
            })
        return [{"unit_name": k, "chapters": v} for k, v in grouped.items()]

    def generate_ids_and_paths(self, structure, subject_key):
        """🧠 DSA MAGIC: ID + UPPERCASE PATH MAPPING"""
        log("DSA", "Running Uppercase Path & ID Generation Algorithm...")
        
        subject_key = self.clean_filename(subject_key) 
        flat_map = []
        global_id = 101
        unit_counter = 1

        for unit in structure:
            safe_unit = f"{unit_counter:02d}_{self.clean_filename(unit['unit_name'])}"
            chap_counter = 1
            for chap in unit['chapters']:
                safe_chap = f"{chap_counter:02d}_{self.clean_filename(chap['chapter'])}"
                drive_path = f"{subject_key}/{safe_unit}/{safe_chap}"
                
                chap['id'] = str(global_id)
                chap['drive_path'] = drive_path
                chap['full_unit_name'] = safe_unit 
                
                flat_map.append(chap)
                global_id += 1
                chap_counter += 1
            unit_counter += 1
            
        log("DSA", f"Mapping Complete: {len(flat_map)} IDs generated.")
        return structure

    def execute(self):
        log("INFO", "🚀 ARCHITECT ENGINE STARTED")
        
        # Scan Input Folder
        book_folders = list_files_in_folder(self.input_id)
        book_folders = [f for f in book_folders if f['mimeType'] == 'application/vnd.google-apps.folder']

        if not book_folders:
            log("WARNING", "No Book Folders found in 00_Input. Please upload one.")
            return

        for folder in book_folders:
            raw_subject_name = folder['name']
            subject_key = self.clean_filename(raw_subject_name)
            log("INFO", f"📂 Processing Subject: {raw_subject_name} -> {subject_key}")
            
            files = list_files_in_folder(folder['id'])
            file_map = {f['name']: f['id'] for f in files}

            # Detect Method
            method_type = "UNKNOWN"
            if 'SYLLABUS_DB.json' in file_map: method_type = "METHOD_2"
            elif any(f.lower().endswith('index.pdf') for f in file_map): method_type = "METHOD_1"
            
            log("INFO", f"⚙️ Mode Detected: {method_type}")

            # Process Assets
            prompt_text = "Generate MCQs."
            if 'MASTER_PROMPT.txt' in file_map:
                download_file(file_map['MASTER_PROMPT.txt'], 'prompt.txt')
                with open('prompt.txt', 'r', encoding='utf-8') as f: prompt_text = f.read()
                os.remove('prompt.txt')

            if 'server_driven_ui.json' in file_map:
                log("INFO", "Syncing SDUI to Firebase...")
                download_file(file_map['server_driven_ui.json'], 'sdui.json')
                with open('sdui.json', 'r', encoding='utf-8') as f: sdui = json.load(f)
                db.reference(f'Syllabus/{subject_key}/Config/UI').set(sdui)
                os.remove('sdui.json')

            # Generate Structure
            structure = []
            main_pdf_id = None
            
            if method_type == "METHOD_1":
                pdf_name = f"{raw_subject_name}.pdf"
                if pdf_name in file_map: main_pdf_id = file_map[pdf_name]
                else:
                    for f in files:
                        if f['name'].endswith('.pdf') and 'index' not in f['name'].lower():
                            main_pdf_id = f['id']; break
                
                index_name = next((k for k in file_map if 'index' in k.lower()), None)
                if index_name:
                    download_file(file_map[index_name], 'index.pdf')
                    structure = self.analyze_index_method_1('index.pdf')
                    os.remove('index.pdf')

            elif method_type == "METHOD_2":
                download_file(file_map['SYLLABUS_DB.json'], 'syllabus.json')
                with open('syllabus.json', 'r', encoding='utf-8') as f: db_data = json.load(f)
                structure = self.process_db_method_2(db_data)
                os.remove('syllabus.json')

            # Save Blueprint
            if structure:
                structure = self.generate_ids_and_paths(structure, subject_key)
                
                # Firebase Sync
                try:
                    db.reference(f'Syllabus/{subject_key}/Structure').set(structure)
                    log("SUCCESS", "Firebase Structure Synced.")
                except: log("WARNING", "Skipped Firebase Sync (Connection issue)")

                blueprint = {
                    "meta": {
                        "subject_key": subject_key,
                        "mode": method_type,
                        "main_pdf_id": main_pdf_id,
                        "created_at": int(time.time())
                    },
                    "prompt": prompt_text,
                    "structure": structure
                }
                
                upload_json(blueprint, f"{subject_key}_BLUEPRINT.json", self.blueprint_id)
                log("SUCCESS", f"Phase 1 Complete. Blueprint Ready: {subject_key}_BLUEPRINT.json")
            else:
                log("ERROR", "Structure Generation Failed.")

if __name__ == "__main__":
    try:
        AJXArchitect().execute()
    except Exception as e:
        log("CRITICAL", f"Unhandled Script Error: {e}")
        sys.exit(1)
