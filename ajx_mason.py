import os
import json
import time
import shutil
import re
import sys
import fitz  # PyMuPDF
import io
import gc  # Garbage Collector
import random
import argparse
import concurrent.futures
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from googleapiclient.errors import HttpError

# ==========================================
# ⚙️ CONFIGURATION
# ==========================================
DRIVE_ROOT_ID = "1i_YALAikZVwKmSlor6QgF5dm-0hhMxSw"
SCOPES = ['https://www.googleapis.com/auth/drive']
MAX_LOCAL_WORKERS = 1  # 1 Thread per VM (We rely on 5 VMs for speed)

def log(level, msg):
    """Custom Logger"""
    icons = {
        "INFO": "ℹ️", "SUCCESS": "✅", "WARNING": "⚠️", 
        "ERROR": "❌", "CRITICAL": "💀", "MASON": "🧱", 
        "SKIP": "⏭️", "AUDIT": "🧐", "RETRY": "⏳"
    }
    print(f"{icons.get(level, '')} [{level}] {msg}",flush=True)

# ==========================================
# 🛡️ DSA: EXPONENTIAL BACKOFF DECORATOR
# ==========================================
def retry_with_backoff(func):
    """Wraps Google API calls with Exponential Backoff + Jitter."""
    def wrapper(*args, **kwargs):
        retries = 0
        max_retries = 8
        base_delay = 1.5
        while retries < max_retries:
            try:
                return func(*args, **kwargs)
            except HttpError as e:
                if e.resp.status in [403, 429, 500, 502, 503]:
                    retries += 1
                    jitter = random.uniform(0, 1.0)
                    sleep_time = (base_delay * (2 ** retries)) + jitter
                    log("RETRY", f"⚠️ API Throttle ({e.resp.status}). Sleeping {sleep_time:.2f}s...")
                    time.sleep(sleep_time)
                else: raise e
            except Exception as e:
                if "socket" in str(e).lower() or "connection" in str(e).lower():
                    retries += 1
                    sleep_time = (base_delay * (2 ** retries)) + random.uniform(0, 1.0)
                    log("RETRY", f"⚠️ Connection Glitch. Sleeping {sleep_time:.2f}s...")
                    time.sleep(sleep_time)
                else: raise e
        log("CRITICAL", f"❌ Max retries exceeded for {func.__name__}")
        raise Exception("Max Retries Exceeded")
    return wrapper

# ==========================================
# 🔐 AUTHENTICATION
# ==========================================
def setup_auth():
    log("INFO", "Initializing Auth Logic...")
    oauth_json = os.environ.get("GDRIVE_OAUTH_JSON")
    if oauth_json:
        try:
            token_info = json.loads(oauth_json)
            creds = Credentials.from_authorized_user_info(token_info, SCOPES)
            return build('drive', 'v3', credentials=creds)
        except: pass
    sa_json = os.environ.get("GDRIVE_CREDENTIALS")
    if sa_json:
        try:
            creds = service_account.Credentials.from_service_account_info(json.loads(sa_json), scopes=SCOPES)
            return build('drive', 'v3', credentials=creds)
        except: pass
    sys.exit(1)

service = setup_auth()

# ==========================================
# 📂 DRIVE HELPER FUNCTIONS
# ==========================================
@retry_with_backoff
def get_folder_id(folder_name, parent_id=DRIVE_ROOT_ID):
    if not parent_id: parent_id = DRIVE_ROOT_ID
    query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false and '{parent_id}' in parents"
    results = service.files().list(q=query, fields="files(id)").execute()
    files = results.get('files', [])
    return files[0]['id'] if files else None

@retry_with_backoff
def create_folder(folder_name, parent_id=DRIVE_ROOT_ID):
    query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false and '{parent_id}' in parents"
    results = service.files().list(q=query, fields="files(id)").execute()
    files = results.get('files', [])
    if files: return files[0]['id']
    meta = {'name': folder_name, 'mimeType': 'application/vnd.google-apps.folder', 'parents': [parent_id]}
    folder = service.files().create(body=meta, fields='id').execute()
    return folder.get('id')

@retry_with_backoff
def list_folders_with_ids(parent_id):
    query = f"'{parent_id}' in parents and trashed = false and mimeType = 'application/vnd.google-apps.folder'"
    results = service.files().list(q=query, fields="files(id, name)", pageSize=1000).execute()
    return {f['name']: f['id'] for f in results.get('files', [])}

@retry_with_backoff
def list_files_in_folder(folder_id):
    query = f"'{folder_id}' in parents and trashed = false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    return results.get('files', [])

@retry_with_backoff
def count_files_in_folder(folder_id):
    query = f"'{folder_id}' in parents and trashed = false and mimeType != 'application/vnd.google-apps.folder'"
    results = service.files().list(q=query, fields="files(id)", pageSize=1000).execute()
    return len(results.get('files', []))

@retry_with_backoff
def delete_file(file_id):
    service.files().delete(fileId=file_id).execute()
    return True

@retry_with_backoff
def download_file(file_id, local_name):
    request = service.files().get_media(fileId=file_id)
    fh = io.FileIO(local_name, 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done: status, done = downloader.next_chunk()
    fh.close()
    return True

@retry_with_backoff
def upload_file(local_path, folder_id, mime_type='image/jpeg'):
    filename = os.path.basename(local_path)
    meta = {'name': filename, 'parents': [folder_id]}
    media = MediaFileUpload(local_path, mimetype=mime_type)
    service.files().create(body=meta, media_body=media).execute()
    return True

# ==========================================
# 🧱 THE MASON (Distributed Worker + Offset Logic)
# ==========================================
class AJXMason:
    def __init__(self):
        self.blueprint_folder_id = get_folder_id("01_Blueprints")
        if not self.blueprint_folder_id:
            log("CRITICAL", "01_Blueprints folder not found!")
            sys.exit(1)

        self.masonry_id = create_folder("02_Masonry", DRIVE_ROOT_ID)
        self.work_dir = "mason_work_area"
        
        if os.path.exists(self.work_dir): shutil.rmtree(self.work_dir)
        os.makedirs(self.work_dir)

    def clean_filename(self, text):
        text = text.upper().replace(" ", "_")
        return re.sub(r'[^A-Z0-9_]', '', text)

    def process_chapter_task(self, task_data):
        chapter = task_data['chapter']
        unit_name = task_data['unit_name']
        subject_folder_id = task_data['subject_folder_id']
        subject_key = task_data['subject_key']
        pdf_path = "source.pdf"
        
        # 🔥 GET OFFSET
        global_offset = task_data.get('global_offset', 0)
        
        chap_name = chapter['chapter']
        chap_id = chapter.get('id')
        start_p = chapter.get('start_p', 1)
        end_p = chapter.get('end_p', 1)

        safe_chap_name = self.clean_filename(chap_name)
        folder_name = f"{chap_id}_{safe_chap_name}"
        local_chap_dir = os.path.join(self.work_dir, folder_name)
        
        if os.path.exists(local_chap_dir): shutil.rmtree(local_chap_dir)
        os.makedirs(local_chap_dir, exist_ok=True)

        doc = None
        generated_files = []
        try:
            doc = fitz.open(pdf_path)
            
            # 🔥 APPLY OFFSET (Map Book Page -> PDF Page)
            # Example: Book Pg 10 - Offset 9 = PDF Pg 1 (Index 0)
            corrected_start_p = start_p - global_offset
            corrected_end_p = end_p - global_offset

            # Safety check (Ensure we don't go below page 1)
            if corrected_start_p < 1: corrected_start_p = 1
            
            log("MASON", f"📸 Processing {folder_name}: Book {start_p}-{end_p} -> PDF {corrected_start_p}-{corrected_end_p}")
            
            # Convert to 0-based index for PyMuPDF
            start_idx = max(0, corrected_start_p - 1)
            end_idx = corrected_end_p 
            
            # Clamp to PDF limits
            end_idx = min(len(doc), end_idx)

            img_counter = 1 

            for i in range(start_idx, end_idx):
                page = doc.load_page(i)
                mat = fitz.Matrix(2, 2) 
                pix = page.get_pixmap(matrix=mat)
                
                filename = f"{img_counter}.jpg"
                filepath = os.path.join(local_chap_dir, filename)
                pix.save(filepath)
                generated_files.append(filepath)
                img_counter += 1
                del pix, page

        except Exception as e:
            log("ERROR", f"Failed to convert PDF: {e}")
            return None
        finally:
            if doc: doc.close()
            gc.collect() 

        if generated_files:
            chap_drive_id = create_folder(folder_name, subject_folder_id)

            meta_data = {
                "chapter_id": chap_id,
                "chapter_name": chap_name,
                "unit": unit_name,
                "subject": subject_key,
                "original_start_page": start_p,
                "total_images": len(generated_files),
                "pdf_page_range": f"{corrected_start_p}-{corrected_end_p}"
            }
            
            meta_path = os.path.join(local_chap_dir, "meta.json")
            with open(meta_path, "w") as f: json.dump(meta_data, f, indent=4)
            
            upload_file(meta_path, chap_drive_id, 'application/json')
            for img_path in generated_files:
                upload_file(img_path, chap_drive_id, 'image/jpeg')
            
            log("SUCCESS", f"✅ Uploaded {len(generated_files)} images for {folder_name}")
        else:
            log("WARNING", f"⚠️ No images generated for {folder_name}")

        try: shutil.rmtree(local_chap_dir)
        except: pass
        
        return folder_name

    def process_blueprint(self, blueprint_file, shard_index, total_shards):
        bp_name = blueprint_file['name']
        log("INFO", f"📜 Processing Blueprint: {bp_name}")
        
        download_file(blueprint_file['id'], "blueprint.json")
        with open("blueprint.json", "r") as f: blueprint = json.load(f)
        
        meta = blueprint.get('meta', {})
        subject_key = meta.get('subject_key')
        pdf_id = meta.get('main_pdf_id')
        structure = blueprint.get('structure', [])

        subject_folder_id = create_folder(subject_key, self.masonry_id)
        
        log("AUDIT", f"🔍 Audit: Checking progress for {subject_key}...")
        existing_folders_map = list_folders_with_ids(subject_folder_id)
        
        # 📐 CALCULATE GLOBAL OFFSET
        # Find the absolute first page mentioned in the blueprint
        all_start_pages = []
        for unit in structure:
            for chapter in unit['chapters']:
                all_start_pages.append(chapter.get('start_p', 1))
        
        min_start_p = min(all_start_pages) if all_start_pages else 1
        global_offset = min_start_p - 1 
        
        log("INFO", f"📐 Global Offset: {global_offset} pages (Book Pg {min_start_p} maps to PDF Pg 1)")

        tasks = []
        needs_pdf = False
        
        # 🔢 FLATTEN AND SHARD
        all_chapters_flat = []
        for unit in structure:
            for chapter in unit['chapters']:
                all_chapters_flat.append({ "chapter": chapter, "unit": unit })

        for idx, item in enumerate(all_chapters_flat):
            if idx % total_shards != shard_index: continue # Skip if not my shard
            
            chapter = item['chapter']
            unit = item['unit']
            
            chap_id = chapter.get('id')
            chap_name = chapter['chapter']
            start_p = chapter.get('start_p', 1)
            end_p = chapter.get('end_p', 1)
            safe_chap_name = self.clean_filename(chap_name)
            folder_name = f"{chap_id}_{safe_chap_name}"
            expected_count = (end_p - start_p) + 2

            if folder_name in existing_folders_map:
                folder_id = existing_folders_map[folder_name]
                actual_count = count_files_in_folder(folder_id)

                if actual_count >= expected_count:
                    log("SKIP", f"⏭️ {folder_name} is Complete.")
                    continue 
                else:
                    log("WARNING", f"⚠️ CORRUPTED: {folder_name}. Retrying...")
                    delete_file(folder_id)
            
            needs_pdf = True
            tasks.append({
                "chapter": chapter,
                "unit_name": unit['unit_name'],
                "subject_key": subject_key,
                "subject_folder_id": subject_folder_id,
                "global_offset": global_offset # PASS OFFSET TO WORKER
            })

        if not tasks:
            log("INFO", f"Shard {shard_index+1}: No pending tasks.")
            return

        log("INFO", f"⬇️ Downloading PDF...")
        if needs_pdf: download_file(pdf_id, "source.pdf")

        log("MASON", f"🚀 Shard {shard_index+1} starting {len(tasks)} chapters...")

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_LOCAL_WORKERS) as executor:
            future_to_chap = {executor.submit(self.process_chapter_task, task): task for task in tasks}
            for future in concurrent.futures.as_completed(future_to_chap):
                try: future.result()
                except Exception as exc: log("ERROR", f"Worker Exception: {exc}")

        if os.path.exists("source.pdf"): os.remove("source.pdf")

    def execute(self, shard_index=0, total_shards=1):
        stagger_time = shard_index * 5
        log("INFO", f"⏳ Staggering Start: Sleeping for {stagger_time}s...")
        time.sleep(stagger_time)

        log("INFO", f"🚀 MASON ENGINE STARTED (Shard {shard_index+1}/{total_shards})")
        blueprints = list_files_in_folder(self.blueprint_folder_id)
        
        if not blueprints: return

        for bp in blueprints:
            if bp['name'].endswith('_BLUEPRINT.json'):
                self.process_blueprint(bp, shard_index, total_shards)
        
        if os.path.exists("blueprint.json"): os.remove("blueprint.json")
        if os.path.exists(self.work_dir): shutil.rmtree(self.work_dir)
        log("SUCCESS", "✅ Shard Completed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--shard', type=int, default=0, help='Shard Index (0-based)')
    parser.add_argument('--total', type=int, default=1, help='Total Shards')
    args = parser.parse_args()
    AJXMason().execute(shard_index=args.shard, total_shards=args.total)
