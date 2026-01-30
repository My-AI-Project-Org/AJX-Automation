print("╔════════════════════════════════════════════════════╗")
print("║   AJX PHASE 3: ROBUST SYNC (ANTI-BROKEN PIPE)      ║")
print("╚════════════════════════════════════════════════════╝")

import os
import json
import time
import shutil
import zipfile
import concurrent.futures
import random
import threading
import io
import re

import google.generativeai as genai
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
import firebase_admin
from firebase_admin import credentials, db
from google.api_core import exceptions as google_exceptions
import socket # To handle socket errors

# --- CONFIG ---
INPUT_DIR = "AJX_Phase1_Output"   
OUTPUT_DIR = "AJX_Worker_Output"  
ZIP_DIR = "AJX_Ready_Packages"    
PROMPT_FILE_NAME = 'master_prompt.txt'

# ☁️ DRIVE CONFIG
DRIVE_ROOT_FOLDER_NAME = "AJX_Worker_Output_LIVE"
DRIVE_BACKUP_FOLDER = "AJX_Automated_Backups"     

# ⚙️ GENERATION SETTINGS
MIN_QUESTIONS_TARGET = 30
MAX_QUESTIONS_TARGET = 100 
QUESTIONS_PER_PASS = 25 
MAX_RETRIES_PER_IMG = 3 

# GitHub Env
TOTAL_WORKERS = int(os.environ.get("TOTAL_WORKERS", 3)) 
WORKER_ID = int(os.environ.get("WORKER_ID", 1))

# Set global socket timeout to prevent hanging
socket.setdefaulttimeout(600) 

# --- 📱 TELEGRAM LOGGER ---
class TelegramLogger:
    def __init__(self):
        self.token = os.environ.get("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.environ.get("TELEGRAM_CHAT_ID")
        self.worker_tag = f"[W-{WORKER_ID}]" 

    def log(self, message, notify=False):
        print(f"{self.worker_tag} {message}", flush=True)
        if notify and self.token:
            try:
                import urllib.request, urllib.parse
                full_msg = f"{self.worker_tag} {message}"
                url = f"https://api.telegram.org/bot{self.token}/sendMessage"
                data = urllib.parse.urlencode({"chat_id": self.chat_id, "text": full_msg}).encode()
                urllib.request.urlopen(urllib.request.Request(url, data=data))
                time.sleep(0.3)
            except: pass

logger = TelegramLogger()

# --- SERVICES ---
def init_services():
    try:
        oauth_json = os.environ.get("GDRIVE_OAUTH_JSON")
        if not oauth_json: 
            logger.log("❌ Error: GDRIVE_OAUTH_JSON missing!", notify=True)
            return None
        token_info = json.loads(oauth_json)
        creds = Credentials.from_authorized_user_info(token_info, ['https://www.googleapis.com/auth/drive'])
        drive_service = build('drive', 'v3', credentials=creds)

        if not firebase_admin._apps:
            fb_json = os.environ.get("FIREBASE_SERVICE_ACCOUNT")
            if fb_json:
                cred = credentials.Certificate(json.loads(fb_json))
                firebase_admin.initialize_app(cred, {'databaseURL': os.environ.get("FIREBASE_DB_URL")})
        return drive_service
    except: return None

# --- LOAD KEYS ---
def load_my_keys():
    keys_str = ""
    if WORKER_ID == 1: keys_str = os.environ.get("KEYS_WORKER_1", "[]")
    elif WORKER_ID == 2: keys_str = os.environ.get("KEYS_WORKER_2", "[]")
    elif WORKER_ID == 3: keys_str = os.environ.get("KEYS_WORKER_3", "[]")
    else: keys_str = os.environ.get("GEMINI_API_KEYS_LIST", "[]")

    try:
        keys = json.loads(keys_str)
        if not keys: raise ValueError("Empty List")
        return keys
    except:
        logger.log("❌ CRITICAL: No Keys Found!", notify=True)
        exit(1)

api_keys = load_my_keys()
key_lock = threading.Lock()
current_key_index = 0 

def get_next_key():
    global current_key_index
    with key_lock:
        current_key_index = (current_key_index + 1) % len(api_keys)
        logger.log(f"🔄 Switching Key -> Index {current_key_index}", notify=True)
        return api_keys[current_key_index]

def get_current_key():
    with key_lock: return api_keys[current_key_index]

# --- HELPERS ---
def natural_sort_key(s):
    return [int(text) if text.isdigit() else text.lower() for text in re.split('([0-9]+)', s)]

def clean_json_response(text):
    text = text.strip()
    start = text.find('[')
    end = text.rfind(']')
    if start != -1 and end != -1:
        return text[start : end + 1]
    return text 

# --- DRIVE MIRRORING UTILS ---
folder_cache = {} 

def get_drive_folder_id(drive_service, folder_name, parent_id=None):
    cache_key = f"{parent_id}_{folder_name}"
    if cache_key in folder_cache: return folder_cache[cache_key]
    
    # Retry logic for folder fetching
    for attempt in range(3):
        try:
            query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
            if parent_id: query += f" and '{parent_id}' in parents"
            
            results = drive_service.files().list(q=query, fields="files(id)").execute()
            files = results.get('files', [])
            
            if files:
                f_id = files[0]['id']
            else:
                meta = {'name': folder_name, 'mimeType': 'application/vnd.google-apps.folder'}
                if parent_id: meta['parents'] = [parent_id]
                folder = drive_service.files().create(body=meta, fields='id').execute()
                f_id = folder.get('id')

            folder_cache[cache_key] = f_id
            return f_id
        except Exception as e:
            time.sleep(2)
    return None

def ensure_drive_path_exists(drive_service, relative_path):
    current_parent_id = get_drive_folder_id(drive_service, DRIVE_ROOT_FOLDER_NAME)
    parts = relative_path.split(os.sep)
    for part in parts:
        if part == ".": continue
        current_parent_id = get_drive_folder_id(drive_service, part, current_parent_id)
    return current_parent_id 

def check_file_on_drive(drive_service, filename, parent_id):
    try:
        query = f"name = '{filename}' and '{parent_id}' in parents and trashed = false"
        results = drive_service.files().list(q=query, fields="files(id)").execute()
        files = results.get('files', [])
        if files: return files[0]['id']
    except: pass
    return None

def upload_json_immediately(drive_service, local_path, filename, parent_id):
    # Retry logic for individual file upload
    for attempt in range(3):
        try:
            existing_id = check_file_on_drive(drive_service, filename, parent_id)
            media = MediaFileUpload(local_path, mimetype='application/json')
            if existing_id:
                drive_service.files().update(fileId=existing_id, media_body=media).execute()
            else:
                meta = {'name': filename, 'parents': [parent_id]}
                drive_service.files().create(body=meta, media_body=media).execute()
            return # Success
        except Exception as e:
            time.sleep(2) # Wait and retry
    logger.log(f"❌ Upload Failed {filename} after retries", notify=False)

def download_file_from_drive(drive_service, file_id, local_path):
    try:
        request = drive_service.files().get_media(fileId=file_id)
        fh = io.FileIO(local_path, 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done: _, done = downloader.next_chunk()
        return True
    except: return False

# --- GEMINI CALLER ---
def call_gemini(img_path, prompt, task_type="Generation"):
    max_key_tries = len(api_keys)
    for _ in range(max_key_tries):
        try:
            time.sleep(random.uniform(2, 4)) 
            genai.configure(api_key=get_current_key())
            model = genai.GenerativeModel('gemini-2.5-flash')
            img_file = genai.upload_file(img_path)
            
            wait = 0
            while img_file.state.name == "PROCESSING":
                time.sleep(1)
                img_file = genai.get_file(img_file.name)
                wait += 1
                if wait > 20: raise TimeoutError("Processing Timeout")

            gen_config = {"temperature": 0.4, "top_p": 1, "top_k": 1, "max_output_tokens": 16384}
            response = model.generate_content([prompt, img_file], generation_config=gen_config)
            return response.text
            
        except google_exceptions.ResourceExhausted:
            logger.log(f"🛑 Quota Hit. Waiting 30s...", notify=True)
            time.sleep(5)
            get_next_key()
        except Exception as e:
            if "429" in str(e):
                time.sleep(30)
                get_next_key()
            else:
                return None
    return None

# --- MULTI-PASS LOGIC ---
def analyze_image(img_path):
    prompt = "Analyze this image and estimate the total number of unique, high-quality MCQs possible. Provide only a single integer number as your answer."
    response = call_gemini(img_path, prompt, "Analysis")
    try: return int(response.strip())
    except: return 30 

def generate_questions_multipass(img_path, target_count, master_prompt):
    all_questions = []
    while len(all_questions) < target_count:
        remaining = target_count - len(all_questions)
        batch_size = min(QUESTIONS_PER_PASS, remaining)
        start_id = len(all_questions) + 1
        
        pass_prompt = f"""
        {master_prompt}
        **BATCH INSTRUCTION:**
        - Create exactly {batch_size} NEW unique MCQs.
        - Start Question IDs from: {start_id}
        - Return strictly a JSON array.
        """
        
        logger.log(f"      ↳ Batch: requesting {batch_size} Qs (Total: {len(all_questions)})", notify=True)
        
        batch_success = False
        for attempt in range(2): 
            response = call_gemini(img_path, pass_prompt, "Generation")
            if response:
                clean = clean_json_response(response)
                try:
                    new_qs = json.loads(clean)
                    if isinstance(new_qs, list) and new_qs:
                        all_questions.extend(new_qs)
                        batch_success = True
                        break
                except: pass
            if not batch_success: time.sleep(2)
        if not batch_success: break 
            
    return json.dumps(all_questions, indent=2) if all_questions else None

# --- 🔥 ROBUST SYNC (Retry Logic Added) ---
def sync_chapter_final(drive_service, chapter_name, zip_path, chapter_full_path):
    if not drive_service: return
    
    # 1. RETRY LOOP FOR FIREBASE INJECTION
    for attempt in range(3): # Try 3 times
        try:
            all_data = []
            for root, _, files in os.walk(chapter_full_path):
                files.sort(key=lambda x: natural_sort_key(x))
                for file in files:
                    if file.endswith(".json"):
                        try:
                            with open(os.path.join(root, file), 'r') as f:
                                all_data.extend(json.load(f))
                        except: pass
            
            if all_data:
                for idx, q in enumerate(all_data): q['id'] = idx + 1
                safe_name = re.sub(r'[.#$\[\]]', '_', chapter_name)
                ref = db.reference(f'chapters/{safe_name}')
                ref.set(all_data) # <--- This can fail if network drops
                logger.log(f"🔥 Live DB Injection: {len(all_data)} Qs uploaded.", notify=True)
            break # Success, exit loop
        except Exception as e:
            logger.log(f"⚠️ Firebase Sync Attempt {attempt+1} Failed: {e}. Retrying...", notify=False)
            time.sleep(5) # Wait 5s before retry

    # 2. RETRY LOOP FOR ZIP BACKUP
    for attempt in range(3): # Try 3 times
        try:
            folder_id = get_drive_folder_id(drive_service, DRIVE_BACKUP_FOLDER)
            filename = os.path.basename(zip_path)
            
            # Delete old
            q = f"name = '{filename}' and '{folder_id}' in parents and trashed = false"
            res = drive_service.files().list(q=q, fields="files(id)").execute()
            for f in res.get('files', []):
                try: drive_service.files().delete(fileId=f['id']).execute()
                except: pass

            # Upload new
            meta = {'name': filename, 'parents': [folder_id]}
            media = MediaFileUpload(zip_path, mimetype='application/zip')
            drive_service.files().create(body=meta, media_body=media, fields='id').execute()
            
            logger.log(f"📦 Backup Zip Synced.", notify=True)
            break # Success, exit loop
        except Exception as e:
             logger.log(f"⚠️ Drive Sync Attempt {attempt+1} Failed: {e}. Retrying...", notify=False)
             time.sleep(5)

def zip_chapter_local(chapter_path):
    chapter_name = os.path.basename(chapter_path)
    zip_file = os.path.join(ZIP_DIR, f"{chapter_name}.zip")
    if os.path.exists(zip_file): os.remove(zip_file)
    rel_path = os.path.relpath(chapter_path, INPUT_DIR)
    target_out = os.path.join(OUTPUT_DIR, rel_path)
    if not os.path.exists(target_out): return None
    with zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(target_out):
            for f in files:
                if f.endswith(".json"):
                    zf.write(os.path.join(root, f), os.path.relpath(os.path.join(root, f), OUTPUT_DIR))
    return zip_file

def get_prompt():
    p = [PROMPT_FILE_NAME, os.path.join(os.path.dirname(__file__), PROMPT_FILE_NAME)]
    for x in p:
        if os.path.exists(x): return open(x, 'r', encoding='utf-8').read()
    return "Extract MCQs JSON."

# --- MAIN ---
def main():
    os.makedirs(ZIP_DIR, exist_ok=True)
    drive = init_services() 
    master_prompt = get_prompt()
    
    logger.log(f"🚀 Worker {WORKER_ID} Started (Robust Sync)", notify=True)

    all_chapters = []
    for root, dirs, files in os.walk(INPUT_DIR):
        if any(f.endswith('.jpg') for f in files):
            all_chapters.append(root)
    all_chapters.sort(key=lambda x: natural_sort_key(os.path.basename(x)))

    my_chapters = []
    for i, chap in enumerate(all_chapters):
        if (i % TOTAL_WORKERS) == (WORKER_ID - 1):
            my_chapters.append(chap)

    logger.log(f"📚 Assigned {len(my_chapters)} chapters", notify=True)

    for idx, chapter_path in enumerate(my_chapters):
        chapter_name = os.path.basename(chapter_path)
        logger.log(f"📂 [{idx+1}/{len(my_chapters)}] Starting: {chapter_name}", notify=True)
        
        rel_chap_path = os.path.relpath(chapter_path, INPUT_DIR)
        drive_chap_folder_id = ensure_drive_path_exists(drive, rel_chap_path)
        
        images = [os.path.join(chapter_path, f) for f in os.listdir(chapter_path) if f.endswith('.jpg')]
        images.sort(key=lambda x: natural_sort_key(os.path.basename(x)))
        
        # --- PROCESS IMAGES ---
        for i, img in enumerate(images):
            img_name = os.path.basename(img)
            json_name = img_name.replace(".jpg", ".json")
            rel_path = os.path.relpath(img, INPUT_DIR)
            json_out = os.path.join(OUTPUT_DIR, rel_path).replace(".jpg", ".json")
            os.makedirs(os.path.dirname(json_out), exist_ok=True)

            # SMART RESUME
            drive_file_id = check_file_on_drive(drive, json_name, drive_chap_folder_id)
            if drive_file_id:
                logger.log(f"   ☁️ Found on Drive: {img_name} (Skipping)", notify=True)
                download_file_from_drive(drive, drive_file_id, json_out)
                continue 
            
            # GENERATE
            logger.log(f"   🔍 Analyzing {img_name}...", notify=True)
            est_count = analyze_image(img)
            target = min(MAX_QUESTIONS_TARGET, max(MIN_QUESTIONS_TARGET, est_count))
            logger.log(f"   📊 AI Estimate: {est_count} | 🎯 Final Target: {target} MCQs", notify=True)
            
            success = False
            for attempt in range(MAX_RETRIES_PER_IMG):
                logger.log(f"      ↳ Attempt {attempt+1}/{MAX_RETRIES_PER_IMG}", notify=True)
                result = generate_questions_multipass(img, target, master_prompt)
                if result:
                    with open(json_out, 'w', encoding='utf-8') as f: f.write(result)
                    upload_json_immediately(drive, json_out, json_name, drive_chap_folder_id)
                    logger.log(f"      ✅ Success & Uploaded!", notify=True)
                    success = True
                    break
            
            if not success:
                logger.log(f"      ❌ FAILED: {img_name}", notify=True)

        # --- FINAL SYNC ---
        zp = zip_chapter_local(chapter_path)
        if zp: 
            full_out_path = os.path.join(OUTPUT_DIR, rel_chap_path)
            sync_chapter_final(drive, chapter_name, zp, full_out_path)

    logger.log("✅ WORKER COMPLETE", notify=True)

if __name__ == "__main__":
    main()
