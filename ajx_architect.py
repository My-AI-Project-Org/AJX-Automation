import os
import json
import time
import re
import io
import sys
import google.generativeai as genai
import firebase_admin
from firebase_admin import credentials, db
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

# ==========================================
# ⚙️ CONFIGURATION & SECRETS SETUP
# ==========================================

# 🔴 PASTE YOUR DRIVE FOLDER ID HERE
DRIVE_ROOT_ID = "https://drive.google.com/drive/u/1/folders/1i_YALAikZVwKmSlor6QgF5dm-0hhMxSw" 

SCOPES = ['https://www.googleapis.com/auth/drive']

def log(level, msg):
    """Custom Logger for Debugging"""
    icons = {"INFO": "ℹ️", "SUCCESS": "✅", "WARNING": "⚠️", "ERROR": "❌", "CRITICAL": "💀", "DSA": "🧮"}
    print(f"{icons.get(level, '')} [{level}] {msg}")

def setup_secrets():
    """Decodes GitHub Secrets into local files for authentication"""
    log("INFO", "Decoding Security Keys from Environment...")
    
    try:
        # 1. Firebase Key
        fb_key = os.environ.get("FIREBASE_SERVICE_KEY")
        if fb_key:
            with open("serviceAccountKey.json", "w") as f: f.write(fb_key)
        else:
            log("WARNING", "FIREBASE_SERVICE_KEY not found in Env.")

        # 2. Drive Credentials
        drive_creds = os.environ.get("GDRIVE_CREDENTIALS")
        if drive_creds:
            with open("credentials.json", "w") as f: f.write(drive_creds)
        
        # 3. Drive Token (For CI/CD)
        drive_token = os.environ.get("GDRIVE_OAUTH_JSON")
        if drive_token:
            with open("token.json", "w") as f: f.write(drive_token)

        # 4. Gemini Key
        keys_list = os.environ.get("GEMINI_API_KEYS_LIST")
        if keys_list:
            try:
                keys = json.loads(keys_list)
                return keys[0] if isinstance(keys, list) else keys_list
            except:
                return keys_list
        return None
    except Exception as e:
        log("CRITICAL", f"Secret Setup Failed: {e}")
        sys.exit(1)

# Initialize Secrets
GEMINI_KEY = setup_secrets()
if GEMINI_KEY:
    genai.configure(api_key=GEMINI_KEY)
else:
    log("CRITICAL", "GEMINI_API_KEY is missing. Aborting.")
    sys.exit(1)

# --- FIREBASE INIT ---
if not firebase_admin._apps:
    try:
        cred = credentials.Certificate("serviceAccountKey.json")
        firebase_admin.initialize_app(cred, {'databaseURL': os.environ.get("FIREBASE_DB_URL")})
        log("SUCCESS", "Firebase Connected Successfully.")
    except Exception as e:
        log("ERROR", f"Firebase Connection Failed: {e}")

# ==========================================
# 🚙 GOOGLE DRIVE MANAGER (The Bridge)
# ==========================================
class DriveManager:
    def __init__(self):
        self.creds = None
        self.service = None
        self.authenticate()

    def authenticate(self):
        try:
            if os.path.exists('token.json'):
                self.creds = Credentials.from_authorized_user_file('token.json', SCOPES)
            
            if not self.creds or not self.creds.valid:
                if self.creds and self.creds.expired and self.creds.refresh_token:
                    self.creds.refresh(Request())
                elif os.path.exists('credentials.json'):
                    log("WARNING", "Refreshing Auth using credentials.json...")
                    flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
                else:
                    log("CRITICAL", "No valid Auth (Token/Creds) found for Drive.")
            
            if self.creds:
                self.service = build('drive', 'v3', credentials=self.creds)
                log("SUCCESS", "Google Drive API Authenticated.")
        except Exception as e:
            log("CRITICAL", f"Drive Auth Failed: {e}")

    def list_files(self, folder_id):
        if not self.service: return []
        try:
            query = f"'{folder_id}' in parents and trashed = false"
            results = self.service.files().list(q=query, fields="files(id, name, mimeType)").execute()
            return results.get('files', [])
        except Exception as e:
            log("ERROR", f"List Files Failed for ID {folder_id}: {e}")
            return []

    def download_file(self, file_id, local_name):
        try:
            request = self.service.files().get_media(fileId=file_id)
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

    def upload_json(self, data, filename, folder_id):
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4)
            
            meta = {'name': filename, 'parents': [folder_id]}
            media = MediaFileUpload(filename, mimetype='application/json')
            self.service.files().create(body=meta, media_body=media).execute()
            log("SUCCESS", f"Blueprint Uploaded: {filename}")
            
            if os.path.exists(filename): os.remove(filename)
        except Exception as e:
            log("ERROR", f"Upload Failed ({filename}): {e}")

    def find_folder(self, name, parent_id):
        if not self.service: return []
        query = f"name = '{name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        if parent_id: query += f" and '{parent_id}' in parents"
        results = self.service.files().list(q=query, fields="files(id, name)").execute()
        return results.get('files', [])

# ==========================================
# 🏛️ THE ARCHITECT (Brain of the Operation)
# ==========================================
class AJXArchitect:
    def __init__(self):
        self.drive = DriveManager()
        self.root_id = DRIVE_ROOT_ID
        
        folders = self.drive.find_folder("01_Blueprints", self.root_id)
        if folders:
            self.blueprint_id = folders[0]['id']
            log("INFO", f"Blueprint Storage Found: {self.blueprint_id}")
        else:
            log("CRITICAL", "'01_Blueprints' folder is missing in Drive Root.")
            sys.exit(1)

    # 🔥 UPDATED: FORCE UPPERCASE LOGIC
    def clean_filename(self, text):
        """Sanitizes strings AND Converts to UPPERCASE"""
        text = text.upper() # <-- 1. Force Upper
        text = text.replace(" ", "_") # 2. Replace Spaces
        return re.sub(r'[^A-Z0-9_]', '', text) # 3. Only Keep Uppercase & Numbers

    def analyze_index_method_1(self, index_path):
        """METHOD 1: Gemini PDF Analysis"""
        log("INFO", "Method 1 Detected: Starting AI Index Analysis...")
        
        try:
            file = genai.upload_file(index_path, display_name="Index PDF")
            
            log("INFO", "Waiting for Gemini Processing...")
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
            structure = json.loads(text)
            log("SUCCESS", f"AI Analysis Complete: Found {len(structure)} Units.")
            return structure
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

        structure = [{"unit_name": k, "chapters": v} for k, v in grouped.items()]
        log("SUCCESS", f"DB Conversion Complete: {len(structure)} Units Created.")
        return structure

    def generate_ids_and_paths(self, structure, subject_key):
        """
        🧠 DSA MAGIC: ID + UPPERCASE PATH MAPPING
        """
        log("DSA", "Running Uppercase Path & ID Generation Algorithm...")
        
        # 🔥 Force Subject Key to Uppercase
        subject_key = self.clean_filename(subject_key) 
        
        flat_map = []
        global_id = 101
        unit_counter = 1

        for unit in structure:
            # Clean Unit Name (Will be Uppercase now)
            safe_unit = f"{unit_counter:02d}_{self.clean_filename(unit['unit_name'])}"
            
            chap_counter = 1
            for chap in unit['chapters']:
                # Clean Chapter Name (Will be Uppercase now)
                safe_chap = f"{chap_counter:02d}_{self.clean_filename(chap['chapter'])}"
                
                # 📍 Drive Path (Everything UPPERCASE)
                # Example: UPSI_HISTORY/01_ANCIENT_HISTORY/01_STONE_AGE
                drive_path = f"{subject_key}/{safe_unit}/{safe_chap}"
                
                chap['id'] = str(global_id)
                chap['drive_path'] = drive_path
                chap['full_unit_name'] = safe_unit 
                
                flat_map.append(chap)
                global_id += 1
                chap_counter += 1
            unit_counter += 1
            
        log("DSA", f"Mapping Complete: {len(flat_map)} IDs generated with UPPERCASE paths.")
        return structure

    def execute(self):
        log("INFO", "🚀 ARCHITECT ENGINE STARTED")
        
        # 1. Scan Input
        folders = self.drive.find_folder("00_Input", self.root_id)
        if not folders:
            log("CRITICAL", "'00_Input' folder missing in Drive.")
            return
            
        input_id = folders[0]['id']
        book_folders = self.drive.list_files(input_id)
        book_folders = [f for f in book_folders if f['mimeType'] == 'application/vnd.google-apps.folder']

        if not book_folders:
            log("WARNING", "No Book Folders found in 00_Input.")
            return

        for folder in book_folders:
            raw_subject_name = folder['name']
            # We use this cleaned key for blueprint naming
            subject_key = self.clean_filename(raw_subject_name)
            
            log("INFO", f"📂 Processing Subject: {raw_subject_name} -> {subject_key}")
            
            files = self.drive.list_files(folder['id'])
            file_map = {f['name']: f['id'] for f in files}

            # 2. Detect Method
            method_type = "UNKNOWN"
            if 'SYLLABUS_DB.json' in file_map: method_type = "METHOD_2"
            elif any(f.lower().endswith('index.pdf') for f in file_map): method_type = "METHOD_1"
            
            log("INFO", f"⚙️ Mode Detected: {method_type}")

            # 3. Handle Common Assets
            prompt_text = "Generate MCQs."
            if 'MASTER_PROMPT.txt' in file_map:
                self.drive.download_file(file_map['MASTER_PROMPT.txt'], 'prompt.txt')
                with open('prompt.txt', 'r', encoding='utf-8') as f: prompt_text = f.read()
                os.remove('prompt.txt')

            if 'server_driven_ui.json' in file_map:
                log("INFO", "Syncing SDUI to Firebase...")
                self.drive.download_file(file_map['server_driven_ui.json'], 'sdui.json')
                with open('sdui.json', 'r', encoding='utf-8') as f: sdui = json.load(f)
                # Use Uppercase Key for Firebase Path too
                db.reference(f'Syllabus/{subject_key}/Config/UI').set(sdui)
                os.remove('sdui.json')

            # 4. Generate Structure
            structure = []
            main_pdf_id = None
            
            if method_type == "METHOD_1":
                pdf_name = f"{raw_subject_name}.pdf" # Try finding exact match first
                if pdf_name in file_map: main_pdf_id = file_map[pdf_name]
                else:
                    for f in files:
                        if f['name'].endswith('.pdf') and 'index' not in f['name'].lower():
                            main_pdf_id = f['id']; break
                
                index_name = next(k for k in file_map if 'index' in k.lower())
                self.drive.download_file(file_map[index_name], 'index.pdf')
                structure = self.analyze_index_method_1('index.pdf')
                os.remove('index.pdf')

            elif method_type == "METHOD_2":
                self.drive.download_file(file_map['SYLLABUS_DB.json'], 'syllabus.json')
                with open('syllabus.json', 'r', encoding='utf-8') as f: db_data = json.load(f)
                structure = self.process_db_method_2(db_data)
                os.remove('syllabus.json')

            # 5. Apply ID Logic & Sync
            if structure:
                # Passes UPPERCASE subject_key to DSA Logic
                structure = self.generate_ids_and_paths(structure, subject_key)
                
                log("INFO", "Syncing Skeleton to Firebase...")
                db.reference(f'Syllabus/{subject_key}/Structure').set(structure)
                
                blueprint = {
                    "meta": {
                        "subject_key": subject_key, # UPSI_HISTORY
                        "mode": method_type,
                        "main_pdf_id": main_pdf_id,
                        "created_at": int(time.time())
                    },
                    "prompt": prompt_text,
                    "structure": structure
                }
                
                # Save to Drive (Filename is also Uppercase now)
                self.drive.upload_json(blueprint, f"{subject_key}_BLUEPRINT.json", self.blueprint_id)
                log("SUCCESS", f"Phase 1 Complete. Blueprint Ready: {subject_key}_BLUEPRINT.json")
            else:
                log("ERROR", "Structure Generation Failed. Skipping Blueprint.")

if __name__ == "__main__":
    try:
        AJXArchitect().execute()
    except Exception as e:
        log("CRITICAL", f"Unhandled Script Error: {e}")
        sys.exit(1)
