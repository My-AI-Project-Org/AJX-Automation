print("╔════════════════════════════════════════════════════╗")
print("║   AJX PHASE 3: VERIFICATION MODE (NO MISSING FILES)║")
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

# --- CONFIG ---
INPUT_DIR = "AJX_Phase1_Output"   
OUTPUT_DIR = "AJX_Worker_Output"  
ZIP_DIR = "AJX_Ready_Packages"    
PROMPT_FILE_NAME = 'master_prompt.txt'

# Batching Settings
MIN_QUESTIONS_TARGET = 30
MAX_QUESTIONS_TARGET = 100 
QUESTIONS_PER_PASS = 25 
MAX_RETRIES_PER_IMG = 3 

# GitHub Env
TOTAL_WORKERS = int(os.environ.get("TOTAL_WORKERS", 3)) 
WORKER_ID = int(os.environ.get("WORKER_ID", 1))

# --- TELEGRAM LOGGER ---
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
                formatted_msg = f"<b>{self.worker_tag}</b> {message}"
                url = f"https://api.telegram.org/bot{self.token}/sendMessage"
                data = urllib.parse.urlencode({"chat_id": self.chat_id, "text": formatted_msg, "parse_mode": "HTML"}).encode()
                urllib.request.urlopen(urllib.request.Request(url, data=data))
            except: pass

logger = TelegramLogger()

# --- SERVICES ---
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
        logger.log(f"🔄 Switching Key -> Index {current_key_index}", notify=False)
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

# --- RESTORE ---
def download_previous_work(drive_service, chapter_name):
    if not drive_service: return False
    try:
        query = f"name = '{chapter_name}.zip' and trashed = false"
        results = drive_service.files().list(q=query, fields="files(id)").execute()
        files = results.get('files', [])
        if not files: return False
        
        file_id = files[0]['id']
        save_path = os.path.join(ZIP_DIR, f"RESTORE_{chapter_name}.zip")
        
        request = drive_service.files().get_media(fileId=file_id)
        fh = io.FileIO(save_path, 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done: _, done = downloader.next_chunk()
        fh.close()
        
        with zipfile.ZipFile(save_path, 'r') as zip_ref:
            zip_ref.extractall(OUTPUT_DIR)
        logger.log(f"♻️ Restored work for {chapter_name}", notify=True)
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
            time.sleep(30)
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
        
        logger.log(f"      ↳ Batch: requesting {batch_size} Qs (Total so far: {len(all_questions)})", notify=False)
        
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
            
            if not batch_success:
                time.sleep(2)
        
        if not batch_success:
            break # Stop passes if one fails hard
            
    return json.dumps(all_questions, indent=2) if all_questions else None

# --- SYNC & ZIP ---
def sync_chapter(drive_service, chapter_name, zip_path):
    if not drive_service: return
    try:
        filename = os.path.basename(zip_path)
        q = f"name = '{filename}' and trashed = false"
        res = drive_service.files().list(q=q, fields="files(id)").execute()
        for f in res.get('files', []):
            try: drive_service.files().delete(fileId=f['id']).execute()
            except: pass
            
        meta = {'name': filename}
        media = MediaFileUpload(zip_path, mimetype='application/zip')
        file = drive_service.files().create(body=meta, media_body=media, fields='id').execute()
        
        drive_service.permissions().create(fileId=file['id'], body={'type': 'anyone', 'role': 'reader'}).execute()
        link = f"https://drive.google.com/uc?export=download&id={file['id']}"
        ref = db.reference('updates/latest')
        ref.set({"version": int(time.time()), "url": link, "message": f"Updated: {chapter_name}"})
        
        logger.log(f"🔔 Synced: {chapter_name}", notify=True)
    except: pass

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
                    abs_path = os.path.join(root, f)
                    arcname = os.path.relpath(abs_path, OUTPUT_DIR)
                    zf.write(abs_path, arcname)
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
    
    logger.log(f"🚀 Worker {WORKER_ID} Started (Self-Verify Mode)", notify=True)

    all_chapters = []
    for root, dirs, files in os.walk(INPUT_DIR):
        if any(f.endswith('.jpg') for f in files):
            all_chapters.append(root)
    all_chapters.sort(key=lambda x: natural_sort_key(os.path.basename(x)))

    if not all_chapters:
        logger.log("❌ No chapters found.", notify=True)
        return

    my_chapters = []
    for i, chap in enumerate(all_chapters):
        if (i % TOTAL_WORKERS) == (WORKER_ID - 1):
            my_chapters.append(chap)

    logger.log(f"📚 Assigned {len(my_chapters)} chapters", notify=True)

    for idx, chapter_path in enumerate(my_chapters):
        chapter_name = os.path.basename(chapter_path)
        logger.log(f"📂 [{idx+1}/{len(my_chapters)}] Starting: {chapter_name}", notify=True)
        
        download_previous_work(drive, chapter_name)
        
        images = [os.path.join(chapter_path, f) for f in os.listdir(chapter_path) if f.endswith('.jpg')]
        images.sort(key=lambda x: natural_sort_key(os.path.basename(x)))
        
        # --- PASS 1: STANDARD PROCESSING ---
        for i, img in enumerate(images):
            img_name = os.path.basename(img)
            rel_path = os.path.relpath(img, INPUT_DIR)
            json_out = os.path.join(OUTPUT_DIR, rel_path).replace(".jpg", ".json")
            
            if os.path.exists(json_out):
                logger.log(f"   ⏭️ Skipped: {img_name}", notify=False)
                continue
            
            os.makedirs(os.path.dirname(json_out), exist_ok=True)
            logger.log(f"   🔍 Analyzing {img_name}...", notify=False)
            est_count = analyze_image(img)
            target = min(MAX_QUESTIONS_TARGET, max(MIN_QUESTIONS_TARGET, est_count))
            
            # Retry Loop
            for attempt in range(MAX_RETRIES_PER_IMG):
                logger.log(f"      ↳ Attempt {attempt+1}/{MAX_RETRIES_PER_IMG}", notify=False)
                result = generate_questions_multipass(img, target, master_prompt)
                if result:
                    with open(json_out, 'w', encoding='utf-8') as f: f.write(result)
                    logger.log(f"      ✅ Success!", notify=False)
                    break
        
        # --- 🕵️‍♂️ PASS 2: FINAL AUDIT (VERIFICATION) ---
        # "Zip karne se pehle check karo ki sab kuch hai ya nahi"
        missing_images = []
        for img in images:
            rel_path = os.path.relpath(img, INPUT_DIR)
            json_out = os.path.join(OUTPUT_DIR, rel_path).replace(".jpg", ".json")
            if not os.path.exists(json_out):
                missing_images.append(img)
        
        if missing_images:
            logger.log(f"⚠️ Chapter Incomplete! {len(missing_images)} images failed. Retrying...", notify=True)
            
            for img in missing_images:
                img_name = os.path.basename(img)
                rel_path = os.path.relpath(img, INPUT_DIR)
                json_out = os.path.join(OUTPUT_DIR, rel_path).replace(".jpg", ".json")
                
                logger.log(f"   🔄 Final Retry for: {img_name}", notify=False)
                # Force attempt with default safety target
                result = generate_questions_multipass(img, 30, master_prompt) 
                if result:
                    with open(json_out, 'w', encoding='utf-8') as f: f.write(result)
                    logger.log(f"      ✅ Recovered: {img_name}", notify=False)
                else:
                    logger.log(f"      ❌ Give Up: {img_name} is broken/unreadable", notify=False)

        # --- SYNC ---
        # Ab jo bhi haal hai, Zip karke bhej do
        zp = zip_chapter_local(chapter_path)
        if zp: sync_chapter(drive, chapter_name, zp)

    logger.log("✅ WORKER COMPLETE", notify=True)

if __name__ == "__main__":
    main()
