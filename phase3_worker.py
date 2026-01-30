print("╔════════════════════════════════════════════════════╗")
print("║   AJX PHASE 3: DIAMOND (FINAL WORKER MATRIX)       ║")
print("╚════════════════════════════════════════════════════╝")

import os
import json
import time
import shutil
import zipfile
import concurrent.futures
import random
import re
import urllib.request
import urllib.parse
from collections import deque
import threading
import io

# External Libs
import google.generativeai as genai
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
import firebase_admin
from firebase_admin import credentials, db
from google.api_core import exceptions as google_exceptions

# --- CONFIG ---
INPUT_DIR = "AJX_Phase1_Output"   # Source Images (Nested)
OUTPUT_DIR = "AJX_Worker_Output"  # JSON Output
ZIP_DIR = "AJX_Ready_Packages"    # Zips for Drive
PROMPT_FILE_NAME = 'master_prompt.txt'

# Matrix Config (GitHub Variables)
TOTAL_WORKERS = int(os.environ.get("TOTAL_WORKERS", 1)) 
WORKER_ID = int(os.environ.get("WORKER_ID", 1))

# --- 🟢 TELEGRAM TERMINAL ---
class TelegramTerminal:
    def __init__(self):
        self.token = os.environ.get("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.environ.get("TELEGRAM_CHAT_ID")
        self.message_id = None
        self.last_update_time = 0
        self.log_buffer = deque(maxlen=10)

    def start(self):
        if not self.token: return
        self.message_id = self._send_new(f"<b>🏭 AJX WORKER {WORKER_ID}/{TOTAL_WORKERS}</b>\nDiamond Matrix Started...")

    def log_stream(self, msg):
        print(msg) 
        sys.stdout.flush()
        clean_msg = str(msg).replace("<", "&lt;").replace(">", "&gt;") 
        self.log_buffer.append(f"> {clean_msg}")
        self._refresh_display()

    def update_progress(self, percent, status):
        self._refresh_display(percent, status)

    def _refresh_display(self, percent=None, status="Processing"):
        if time.time() - self.last_update_time < 2.0: return
        if not self.token or not self.message_id: return
        
        logs_text = "\n".join(self.log_buffer)
        text = (f"<b>🏭 AJX WORKER {WORKER_ID}/{TOTAL_WORKERS}</b>\n"
                f"<code>{logs_text}</code>\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"<b>{status}</b>")
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

import sys
terminal = TelegramTerminal()

def log(msg):
    terminal.log_stream(msg)

# --- SETUP SERVICES ---
def init_services():
    try:
        oauth_json = os.environ.get("GDRIVE_OAUTH_JSON")
        if not oauth_json: return None
        token_info = json.loads(oauth_json)
        creds = Credentials.from_authorized_user_info(token_info, ['https://www.googleapis.com/auth/drive'])
        drive_service = build('drive', 'v3', credentials=creds)

        if not firebase_admin._apps:
            fb_json = os.environ.get("FIREBASE_SERVICE_ACCOUNT")
            if fb_json:
                cred = credentials.Certificate(json.loads(fb_json))
                firebase_admin.initialize_app(cred, {'databaseURL': os.environ.get("FIREBASE_DB_URL")})
        return drive_service
    except Exception as e:
        log(f"⚠️ Service Init Error: {e}")
        return None

# --- API KEY MANAGEMENT (SMART ROTATION) ---
api_keys = json.loads(os.environ.get("GEMINI_API_KEYS_LIST", "[]"))
if not isinstance(api_keys, list) or not api_keys:
    log("❌ FATAL: No API Keys found")
    exit()

key_lock = threading.Lock()
# Start with a key specific to this worker to avoid collision at start
current_key_index = (WORKER_ID - 1) % len(api_keys)

def get_next_key():
    global current_key_index
    with key_lock:
        current_key_index = (current_key_index + 1) % len(api_keys)
        new_key = api_keys[current_key_index]
        log(f"🔄 Switching Key -> Index {current_key_index} (...{new_key[-4:]})")
        return new_key

def get_current_key():
    with key_lock:
        return api_keys[current_key_index]

# --- 🧠 MEMORY RECALL (RESTORE FROM DRIVE) ---
def download_previous_work(drive_service, chapter_name, local_target_dir):
    """Checks Drive for existing Zip of this chapter and restores it."""
    if not drive_service: return False
    
    # Check specifically for this chapter's zip
    query = f"name = '{chapter_name}.zip' and trashed = false"
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get('files', [])
    
    if not files:
        return False # No previous work found
    
    # Download the file
    file_to_download = files[0] 
    file_id = file_to_download['id']
    save_path = os.path.join(ZIP_DIR, f"RESTORE_{chapter_name}.zip")
    
    log(f"📥 Found existing progress: {chapter_name}. Restoring...")
    
    try:
        request = drive_service.files().get_media(fileId=file_id)
        fh = io.FileIO(save_path, 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done: status, done = downloader.next_chunk()
        fh.close()
        
        # Unzip to Output Dir to skip already processed files
        with zipfile.ZipFile(save_path, 'r') as zip_ref:
            zip_ref.extractall(OUTPUT_DIR) 
            
        log(f"♻️ Restored files for {chapter_name}")
        return True
    except Exception as e:
        log(f"⚠️ Restore failed (Starting Fresh): {e}")
        return False

# --- ROBUST GENERATION ---
def generate_mcq_robust(img_path, prompt):
    # Try looping through all keys if one fails due to quota
    max_key_tries = len(api_keys) + 1 
    
    for key_attempt in range(max_key_tries):
        try:
            active_key = get_current_key()
            genai.configure(api_key=active_key)
            model = genai.GenerativeModel('gemini-2.5-flash')

            img_file = genai.upload_file(img_path)
            
            # Wait for processing
            wait_time = 0
            while img_file.state.name == "PROCESSING":
                time.sleep(1)
                img_file = genai.get_file(img_file.name)
                wait_time += 1
                if wait_time > 20: raise TimeoutError("Processing Timeout")

            response = model.generate_content([prompt, img_file])
            text = response.text.replace("```json", "").replace("```", "").strip()
            
            if text.startswith("{") or text.startswith("["):
                return text
            else:
                raise ValueError("Invalid JSON")

        except google_exceptions.ResourceExhausted:
            log(f"🚫 Quota Hit on Key {current_key_index}. Switching...")
            get_next_key()
            time.sleep(2)
            continue # Try again with new key

        except Exception as e:
            if "429" in str(e): # Rate limit disguised
                get_next_key()
                continue
            
            log(f"⚠️ Error {os.path.basename(img_path)}: {e}")
            if key_attempt > 1: return None # Give up on this image
            time.sleep(5)
            
    return None 

# --- ZIP, CLEAN, & NOTIFY ---
def send_notification(drive_service, chapter_name, zip_path):
    if not drive_service: return
    try:
        filename = os.path.basename(zip_path)
        
        # 1. DELETE OLD FILE (Clean Drive)
        query = f"name = '{filename}' and trashed = false"
        results = drive_service.files().list(q=query, fields="files(id)").execute()
        for f in results.get('files', []):
            try: 
                drive_service.files().delete(fileId=f['id']).execute()
                log(f"🧹 Deleted old Cloud Zip: {filename}")
            except: pass

        # 2. UPLOAD NEW
        file_metadata = {'name': filename}
        media = MediaFileUpload(zip_path, mimetype='application/zip')
        file = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        
        # 3. PUBLIC LINK
        drive_service.permissions().create(fileId=file['id'], body={'type': 'anyone', 'role': 'reader'}).execute()
        link = f"https://drive.google.com/uc?export=download&id={file['id']}"
        
        # 4. FIREBASE TRIGGER
        ref = db.reference('updates/latest')
        ref.set({
            "version": int(time.time()),
            "url": link,
            "message": f"Updated: {chapter_name}"
        })
        log(f"🔔 Synced: {chapter_name}")
    except Exception as e:
        log(f"⚠️ Sync Error: {e}")

def zip_chapter(chapter_path, relative_root):
    chapter_name = os.path.basename(chapter_path)
    zip_filename = os.path.join(ZIP_DIR, f"{chapter_name}.zip")
    
    # Delete local old zip if exists
    if os.path.exists(zip_filename):
        try: os.remove(zip_filename)
        except: pass

    # Calculate structure
    rel_path = os.path.relpath(chapter_path, INPUT_DIR)
    target_output_dir = os.path.join(OUTPUT_DIR, rel_path)
    
    if not os.path.exists(target_output_dir): return None
            
    has_files = False
    with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(target_output_dir):
            for file in files:
                if file.endswith(".json"):
                    full_path = os.path.join(root, file)
                    # Maintain hierarchy inside zip
                    arcname = os.path.relpath(full_path, OUTPUT_DIR)
                    zipf.write(full_path, arcname)
                    has_files = True
    
    return zip_filename if has_files else None

def get_prompt_content():
    paths = [PROMPT_FILE_NAME, os.path.join(os.path.dirname(__file__), PROMPT_FILE_NAME)]
    for p in paths:
        if os.path.exists(p): return open(p, 'r', encoding='utf-8').read()
    return "Extract MCQs in JSON format."

# --- MAIN WORKER ---
def main():
    terminal.start()
    os.makedirs(ZIP_DIR, exist_ok=True)
    drive_service = init_services()
    master_prompt = get_prompt_content()

    log(f"🔑 Worker {WORKER_ID} ready. Key Index: {current_key_index}")

    # 1. DEEP SEARCH (Nested Support)
    all_chapters = []
    for root, dirs, files in os.walk(INPUT_DIR):
        if any(f.endswith('.jpg') for f in files):
            all_chapters.append(root)
    
    all_chapters.sort() # Serial Order
    
    if not all_chapters:
        log("❌ No chapters found.")
        return

    # 2. MATRIX ASSIGNMENT (Chunking Logic)
    my_chapters = []
    for i, chap in enumerate(all_chapters):
        if (i % TOTAL_WORKERS) == (WORKER_ID - 1):
            my_chapters.append(chap)

    log(f"📚 Total: {len(all_chapters)} | My Load: {len(my_chapters)}")

    # 3. PROCESS LOOP
    for i, chapter_path in enumerate(my_chapters):
        chapter_name = os.path.basename(chapter_path)
        rel_name = os.path.relpath(chapter_path, INPUT_DIR)
        
        log(f"📂 [{i+1}/{len(my_chapters)}] Processing: {rel_name}")
        
        # 🧠 MEMORY CHECK: Restore from Drive
        download_previous_work(drive_service, chapter_name, OUTPUT_DIR)
        
        images = sorted([os.path.join(chapter_path, f) for f in os.listdir(chapter_path) if f.endswith('.jpg')])
        
        # 3-Thread Execution for Safety
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = {}
            for img in images:
                rel_path_img = os.path.relpath(img, INPUT_DIR)
                json_out = os.path.join(OUTPUT_DIR, rel_path_img).replace(".jpg", ".json")
                
                # SKIP if already done (restored)
                if os.path.exists(json_out): continue
                
                os.makedirs(os.path.dirname(json_out), exist_ok=True)
                futures[executor.submit(generate_mcq_robust, img, master_prompt)] = json_out
            
            # Save Results
            for future in concurrent.futures.as_completed(futures):
                path = futures[future]
                result = future.result()
                if result:
                    with open(path, 'w', encoding='utf-8') as f: f.write(result)
        
        # ✅ IMMEDIATE SYNC: Zip, Clean Cloud, Upload New
        zip_path = zip_chapter(chapter_path, INPUT_DIR)
        if zip_path and drive_service:
            send_notification(drive_service, chapter_name, zip_path)
        
        time.sleep(1)

    terminal.update_progress(100, "✅ WORKER DONE")
    log("🎉 Worker Completed Successfully!")

if __name__ == "__main__":
    main()
