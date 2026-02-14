import os
import json
import time
import shutil
import re
import sys
import fitz  # PyMuPDF
import io
import concurrent.futures  # For Multi-threading
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

# ==========================================
# ⚙️ CONFIGURATION (PLATINUM EDITION)
# ==========================================

# 🔴 HARDCODED DRIVE ROOT ID (Your AJX_Factory Folder)
DRIVE_ROOT_ID = "1i_YALAikZVwKmSlor6QgF5dm-0hhMxSw"

SCOPES = ['https://www.googleapis.com/auth/drive']
MAX_WORKERS = 5  # 🔥 5 Parallel Workers (Safe for Drive API)

def log(level, msg):
    """Custom Logger"""
    icons = {
        "INFO": "ℹ️", "SUCCESS": "✅", "WARNING": "⚠️", 
        "ERROR": "❌", "CRITICAL": "💀", "MASON": "🧱", 
        "SKIP": "⏭️", "AUDIT": "🧐"
    }
    print(f"{icons.get(level, '')} [{level}] {msg}")

# ==========================================
# 🔐 AUTHENTICATION
# ==========================================
def setup_auth():
    log("INFO", "Initializing Auth Logic...")
    
    # 1. Try Human OAuth (Priority - Faster & Unlimited)
    oauth_json = os.environ.get("GDRIVE_OAUTH_JSON")
    if oauth_json:
        try:
            token_info = json.loads(oauth_json)
            creds = Credentials.from_authorized_user_info(token_info, SCOPES)
            return build('drive', 'v3', credentials=creds)
        except Exception as e:
            log("WARNING", f"Human Auth Failed: {e}")

    # 2. Fallback to Robot (Service Account)
    sa_json = os.environ.get("GDRIVE_CREDENTIALS")
    if sa_json:
        try:
            creds = service_account.Credentials.from_service_account_info(
                json.loads(sa_json), scopes=SCOPES)
            return build('drive', 'v3', credentials=creds)
        except Exception as e:
            log("WARNING", f"Service Account Failed: {e}")

    log("CRITICAL", "FATAL: No valid Drive Auth found.")
    sys.exit(1)

service = setup_auth()

# ==========================================
# 📂 DRIVE HELPER FUNCTIONS
# ==========================================
def get_folder_id(folder_name, parent_id=DRIVE_ROOT_ID):
    if not parent_id: parent_id = DRIVE_ROOT_ID
    query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false and '{parent_id}' in parents"
    results = service.files().list(q=query, fields="files(id)").execute()
    files = results.get('files', [])
    return files[0]['id'] if files else None

def create_folder(folder_name, parent_id=DRIVE_ROOT_ID):
    existing = get_folder_id(folder_name, parent_id)
    if existing: return existing
    meta = {'name': folder_name, 'mimeType': 'application/vnd.google-apps.folder', 'parents': [parent_id]}
    folder = service.files().create(body=meta, fields='id').execute()
    return folder.get('id')

def list_folders_with_ids(parent_id):
    """Returns Dict { 'FOLDER_NAME': 'FOLDER_ID' } for fast audits"""
    query = f"'{parent_id}' in parents and trashed = false and mimeType = 'application/vnd.google-apps.folder'"
    results = service.files().list(q=query, fields="files(id, name)", pageSize=1000).execute()
    return {f['name']: f['id'] for f in results.get('files', [])}

def count_files_in_folder(folder_id):
    """Returns number of actual files inside a folder"""
    query = f"'{folder_id}' in parents and trashed = false and mimeType != 'application/vnd.google-apps.folder'"
    results = service.files().list(q=query, fields="files(id)", pageSize=1000).execute()
    return len(results.get('files', []))

def delete_file(file_id):
    try:
        service.files().delete(fileId=file_id).execute()
        return True
    except: return False

def download_file(file_id, local_name):
    try:
        request = service.files().get_media(fileId=file_id)
        fh = io.FileIO(local_name, 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done: status, done = downloader.next_chunk()
        fh.close()
        return True
    except Exception as e:
        log("ERROR", f"Download Failed: {e}")
        return False

def upload_file(local_path, folder_id, mime_type='image/jpeg'):
    try:
        filename = os.path.basename(local_path)
        meta = {'name': filename, 'parents': [folder_id]}
        media = MediaFileUpload(local_path, mimetype=mime_type)
        service.files().create(body=meta, media_body=media).execute()
        return True
    except Exception as e:
        log("ERROR", f"Upload Failed ({local_path}): {e}")
        return False

# ==========================================
# 🧱 THE MASON (PDF -> Images + Meta)
# ==========================================
class AJXMason:
    def __init__(self):
        log("INFO", f"Mason initializing with {MAX_WORKERS} Workers...")
        self.blueprint_folder_id = get_folder_id("01_Blueprints")
        if not self.blueprint_folder_id:
            log("CRITICAL", "01_Blueprints folder not found!")
            sys.exit(1)

        self.masonry_id = create_folder("02_Masonry", DRIVE_ROOT_ID)
        self.work_dir = "mason_work_area"
        
        # Fresh Start for Temp Dir
        if os.path.exists(self.work_dir): shutil.rmtree(self.work_dir)
        os.makedirs(self.work_dir)

    def clean_filename(self, text):
        text = text.upper().replace(" ", "_")
        return re.sub(r'[^A-Z0-9_]', '', text)

    def process_chapter_task(self, task_data):
        """
        Worker Function: Converts 1 Chapter -> Images -> Uploads
        """
        chapter = task_data['chapter']
        unit_name = task_data['unit_name']
        subject_folder_id = task_data['subject_folder_id']
        subject_key = task_data['subject_key']
        pdf_path = "source.pdf"  # Shared Resource
        
        chap_name = chapter['chapter']
        chap_id = chapter.get('id')
        start_p = chapter.get('start_p', 1)
        end_p = chapter.get('end_p', 1)

        safe_chap_name = self.clean_filename(chap_name)
        folder_name = f"{chap_id}_{safe_chap_name}"
        
        # 1. Setup Local Workspace for this Chapter
        local_chap_dir = os.path.join(self.work_dir, folder_name)
        os.makedirs(local_chap_dir, exist_ok=True)

        # 2. Convert PDF Pages to Images
        try:
            doc = fitz.open(pdf_path)
        except Exception as e:
            log("ERROR", f"Could not open source PDF: {e}")
            return None

        # Handle indexing (Blueprint is 1-based, PyMuPDF is 0-based)
        start_idx = max(0, start_p - 1)
        end_idx = min(len(doc), end_p)
        
        img_counter = 1  # 🔥 Force Naming: 1.jpg, 2.jpg...
        generated_files = []
        
        log("MASON", f"📸 Processing {folder_name} (Pages {start_p}-{end_p})...")

        for i in range(start_idx, end_idx):
            try:
                page = doc.load_page(i)
                mat = fitz.Matrix(2, 2) # 2x Zoom for High Quality
                pix = page.get_pixmap(matrix=mat)
                
                filename = f"{img_counter}.jpg"
                filepath = os.path.join(local_chap_dir, filename)
                pix.save(filepath)
                
                generated_files.append(filepath)
                img_counter += 1
            except Exception as e:
                log("ERROR", f"Failed to convert page {i+1}: {e}")

        doc.close()

        # 3. Create Drive Folder & Upload
        if generated_files:
            # Create Folder in Drive
            chap_drive_id = create_folder(folder_name, subject_folder_id)

            # Generate META.JSON (The ID Card)
            meta_data = {
                "chapter_id": chap_id,
                "chapter_name": chap_name,
                "unit": unit_name,
                "subject": subject_key,
                "original_start_page": start_p,
                "total_images": len(generated_files)
            }
            
            meta_path = os.path.join(local_chap_dir, "meta.json")
            with open(meta_path, "w") as f:
                json.dump(meta_data, f, indent=4)
            
            # Upload Meta First
            upload_file(meta_path, chap_drive_id, 'application/json')

            # Upload Images
            for img_path in generated_files:
                upload_file(img_path, chap_drive_id, 'image/jpeg')
            
            log("SUCCESS", f"✅ Uploaded {len(generated_files)} images for {folder_name}")
        else:
            log("WARNING", f"⚠️ No images generated for {folder_name}")

        # Cleanup Local
        try:
            shutil.rmtree(local_chap_dir)
        except: pass
        
        return folder_name

    def process_blueprint(self, blueprint_file):
        bp_name = blueprint_file['name']
        log("INFO", f"📜 Processing Blueprint: {bp_name}")
        
        # 1. Download Blueprint
        if not download_file(blueprint_file['id'], "blueprint.json"):
            return

        with open("blueprint.json", "r") as f: blueprint = json.load(f)
        
        meta = blueprint.get('meta', {})
        subject_key = meta.get('subject_key')
        pdf_id = meta.get('main_pdf_id')
        structure = blueprint.get('structure', [])

        if not subject_key or not pdf_id:
            log("ERROR", "Invalid Blueprint: Missing subject_key or pdf_id.")
            return

        # 2. Setup Subject Folder in 02_Masonry
        subject_folder_id = create_folder(subject_key, self.masonry_id)

        # 3. 🔥 SMART AUDIT: Check Existing Progress
        log("AUDIT", f"🔍 Checking existing progress for {subject_key}...")
        existing_folders_map = list_folders_with_ids(subject_folder_id) # {Name: ID}
        
        tasks = []
        needs_pdf = False

        for unit in structure:
            for chapter in unit['chapters']:
                # Calculate Expected
                chap_name = chapter['chapter']
                chap_id = chapter.get('id')
                start_p = chapter.get('start_p', 1)
                end_p = chapter.get('end_p', 1)
                
                safe_chap_name = self.clean_filename(chap_name)
                folder_name = f"{chap_id}_{safe_chap_name}"

                # Formula: (Pages) + 1 (Meta file)
                expected_count = (end_p - start_p) + 1 + 1 

                # Decision Logic
                if folder_name in existing_folders_map:
                    folder_id = existing_folders_map[folder_name]
                    actual_count = count_files_in_folder(folder_id)

                    if actual_count >= expected_count:
                        # Success: Folder exists and has files
                        log("SKIP", f"⏭️ {folder_name} is Complete ({actual_count} files).")
                        continue 
                    else:
                        # Corruption: Folder exists but is empty or partial
                        log("WARNING", f"⚠️ CORRUPTED: {folder_name} (Expected {expected_count}, Found {actual_count}). Deleting...")
                        delete_file(folder_id)
                        # Fall through to add to tasks
                
                # Add to Tasks
                needs_pdf = True
                tasks.append({
                    "chapter": chapter,
                    "unit_name": unit['unit_name'],
                    "subject_key": subject_key,
                    "subject_folder_id": subject_folder_id
                })

        if not tasks:
            log("SUCCESS", f"🎉 {subject_key} is 100% Complete! Moving to next blueprint...")
            return

        # 4. Download PDF ONCE (Only if needed)
        if needs_pdf:
            log("INFO", f"⬇️ Downloading Source PDF (Shared Resource)...")
            if not download_file(pdf_id, "source.pdf"):
                log("CRITICAL", "Failed to download Source PDF. Aborting.")
                return

        log("MASON", f"🚀 Starting 5 Workers for {len(tasks)} Pending Chapters...")

        # 5. Execute Workers
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_chap = {executor.submit(self.process_chapter_task, task): task for task in tasks}
            
            for future in concurrent.futures.as_completed(future_to_chap):
                try:
                    future.result()
                except Exception as exc:
                    log("ERROR", f"Worker Exception: {exc}")

        # Cleanup Shared PDF
        if os.path.exists("source.pdf"): os.remove("source.pdf")
        log("SUCCESS", f"🏁 Finished Processing {subject_key}")

    def execute(self):
        log("INFO", "🚀 MASON ENGINE STARTED")
        blueprints = list_files_in_folder(self.blueprint_folder_id)
        
        if not blueprints:
            log("WARNING", "No Blueprints found. Run Architect First.")
            return

        for bp in blueprints:
            if bp['name'].endswith('_BLUEPRINT.json'):
                self.process_blueprint(bp)
        
        # Final Cleanup
        if os.path.exists("blueprint.json"): os.remove("blueprint.json")
        if os.path.exists(self.work_dir): shutil.rmtree(self.work_dir)
        log("SUCCESS", "✅ All Tasks Completed.")

if __name__ == "__main__":
    AJXMason().execute()
