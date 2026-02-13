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
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from googleapiclient.errors import HttpError

# ==========================================
# 🛑 CONFIGURATION
# ==========================================
DRIVE_ROOT_ID = "1i_YALAikZVwKmSlOr6QgF5dm-0hhMxSw"  # Your Exact Folder ID
SCOPES = ['https://www.googleapis.com/auth/drive']

def log(level, msg):
    icons = {"INFO": "ℹ️", "SUCCESS": "✅", "WARNING": "⚠️", "ERROR": "❌", "CRITICAL": "💀", "DSA": "🧮"}
    print(f"{icons.get(level, '')} [{level}] {msg}")

# ==========================================
# 🔐 AUTHENTICATION (DIRECT INJECTION)
# ==========================================
def authenticate_drive():
    """Authenticates directly using the JSON string from GitHub Secrets."""
    log("INFO", "🔐 Authenticating Robot...")
    
    sa_json = os.environ.get("GDRIVE_CREDENTIALS")
    if not sa_json:
        log("CRITICAL", "Secret 'GDRIVE_CREDENTIALS' is MISSING.")
        sys.exit(1)

    try:
        # Load JSON directly from String (No file writing needed)
        creds_dict = json.loads(sa_json)
        creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        service = build('drive', 'v3', credentials=creds)
        
        # 🧪 TEST CONNECTION IMMEDIATELY
        log("INFO", f"🤖 Identity: {creds.service_account_email}")
        try:
            folder = service.files().get(fileId=DRIVE_ROOT_ID, fields="name, capabilities").execute()
            log("SUCCESS", f"✅ Connected to Drive Folder: '{folder.get('name')}'")
            
            if not folder['capabilities']['canAddChildren']:
                log("CRITICAL", "❌ READ-ONLY ACCESS! You must make the Robot an 'EDITOR'.")
                sys.exit(1)
                
        except HttpError as e:
            if e.resp.status == 404:
                log("CRITICAL", f"❌ Error 404: Robot cannot find folder '{DRIVE_ROOT_ID}'.")
                log("CRITICAL", "👉 FIX: Share the folder with the email printed above.")
            elif e.resp.status == 403:
                log("CRITICAL", "❌ Error 403: API Not Enabled or Permission Denied.")
            sys.exit(1)
            
        return service
        
    except json.JSONDecodeError:
        log("CRITICAL", "GDRIVE_CREDENTIALS is not valid JSON.")
        sys.exit(1)
    except Exception as e:
        log("CRITICAL", f"Auth Failed: {e}")
        sys.exit(1)

# Initialize Service
service = authenticate_drive()

# --- GEMINI & FIREBASE SETUP ---
def setup_other_services():
    # Gemini
    keys = os.environ.get("GEMINI_API_KEYS_LIST")
    if keys:
        try:
            key = json.loads(keys)[0] if "[" in keys else keys.split(',')[0]
            genai.configure(api_key=key.strip().replace('"', ''))
        except: pass
    else:
        # Fallback Hardcoded
        genai.configure(api_key="AIzaSyDmb1hHM0Qn_BKllH0Ev9xVU1EG8k6_53c")

    # Firebase
    if not firebase_admin._apps:
        fb_key = os.environ.get("FIREBASE_SERVICE_KEY")
        if fb_key:
            try:
                # Firebase requires a file path usually, or dict
                cred = credentials.Certificate(json.loads(fb_key))
                firebase_admin.initialize_app(cred, {'databaseURL': os.environ.get("FIREBASE_DB_URL")})
            except: 
                # Fallback: Create temp file
                with open("fb_temp.json", "w") as f: f.write(fb_key)
                cred = credentials.Certificate("fb_temp.json")
                firebase_admin.initialize_app(cred, {'databaseURL': os.environ.get("FIREBASE_DB_URL")})

setup_other_services()

# ==========================================
# 📂 ROBUST FOLDER LOGIC
# ==========================================
def ensure_folder(name, parent_id):
    query = f"name = '{name}' and mimeType = 'application/vnd.google-apps.folder' and '{parent_id}' in parents and trashed = false"
    results = service.files().list(q=query, fields="files(id)").execute()
    if results.get('files'):
        return results['files'][0]['id']
    else:
        log("INFO", f"Creating folder: {name}")
        meta = {'name': name, 'mimeType': 'application/vnd.google-apps.folder', 'parents': [parent_id]}
        return service.files().create(body=meta, fields='id').execute().get('id')

def list_files(folder_id):
    query = f"'{folder_id}' in parents and trashed = false"
    return service.files().list(q=query, fields="files(id, name, mimeType)").execute().get('files', [])

def download(file_id, name):
    try:
        req = service.files().get_media(fileId=file_id)
        fh = io.FileIO(name, 'wb')
        downloader = MediaIoBaseDownload(fh, req)
        done = False
        while not done: _, done = downloader.next_chunk()
        fh.close()
        return True
    except: return False

def upload_json(data, name, folder_id):
    with open(name, 'w', encoding='utf-8') as f: json.dump(data, f, indent=4)
    meta = {'name': name, 'parents': [folder_id]}
    media = MediaFileUpload(name, mimetype='application/json')
    service.files().create(body=meta, media_body=media).execute()
    log("SUCCESS", f"Uploaded: {name}")

# ==========================================
# 🏛️ ARCHITECT LOGIC
# ==========================================
class AJXArchitect:
    def __init__(self):
        self.bp_id = ensure_folder("01_Blueprints", DRIVE_ROOT_ID)
        self.in_id = ensure_folder("00_Input", DRIVE_ROOT_ID)

    def clean_name(self, text):
        return re.sub(r'[^A-Z0-9_]', '', text.upper().replace(" ", "_"))

    def analyze_pdf(self, path):
        log("INFO", "Asking Gemini to analyze PDF...")
        try:
            f = genai.upload_file(path)
            while f.state.name == "PROCESSING": time.sleep(2); f = genai.get_file(f.name)
            model = genai.GenerativeModel("gemini-2.0-flash")
            res = model.generate_content([f, "Extract Syllabus JSON: [{'unit_name': 'X', 'chapters': [{'chapter': 'Y', 'start_p': 1, 'end_p': 10}]}]"])
            return json.loads(res.text.replace("```json", "").replace("```", "").strip())
        except Exception as e:
            log("ERROR", f"Gemini Error: {e}")
            return []

    def execute(self):
        log("INFO", "🚀 Architect Started")
        
        # Check Input
        folders = [f for f in list_files(self.in_id) if f['mimeType'] == 'application/vnd.google-apps.folder']
        if not folders:
            log("WARNING", "00_Input is empty. Upload a Subject Folder.")
            return

        for folder in folders:
            subject = self.clean_name(folder['name'])
            log("INFO", f"Processing Subject: {subject}")
            
            files = {f['name']: f['id'] for f in list_files(folder['id'])}
            structure = []
            main_pdf_id = None
            
            # Find PDF
            for name, fid in files.items():
                if name.endswith('.pdf') and 'index' not in name.lower():
                    main_pdf_id = fid
                    break
            
            # Method 1: PDF Index
            idx_name = next((k for k in files if 'index' in k.lower()), None)
            if idx_name:
                download(files[idx_name], 'index.pdf')
                structure = self.analyze_pdf('index.pdf')
            
            # Method 2: DB
            elif 'SYLLABUS_DB.json' in files:
                download(files['SYLLABUS_DB.json'], 'db.json')
                with open('db.json') as f: db_data = json.load(f)
                # Simple conversion logic
                grouped = {}
                for x in db_data:
                    u = x.get('chapter', 'General')
                    if u not in grouped: grouped[u] = []
                    grouped[u].append({"chapter": x.get('topic', 'Topic'), "start_p": 0, "end_p": 0})
                structure = [{"unit_name": k, "chapters": v} for k,v in grouped.items()]

            if structure:
                # DSA Logic
                gid = 101
                for u_i, unit in enumerate(structure, 1):
                    s_unit = f"{u_i:02d}_{self.clean_name(unit['unit_name'])}"
                    for c_i, chap in enumerate(unit['chapters'], 1):
                        s_chap = f"{c_i:02d}_{self.clean_name(chap['chapter'])}"
                        chap['id'] = str(gid)
                        chap['drive_path'] = f"{subject}/{s_unit}/{s_chap}"
                        gid += 1
                
                bp = {"meta": {"subject": subject, "pdf_id": main_pdf_id}, "structure": structure}
                upload_json(bp, f"{subject}_BLUEPRINT.json", self.bp_id)
                
                # Sync Firebase
                try: db.reference(f'Syllabus/{subject}/Structure').set(structure)
                except: pass

if __name__ == "__main__":
    AJXArchitect().execute()
