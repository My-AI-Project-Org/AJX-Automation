print("╔════════════════════════════════════════════════════╗")
print("║   AJX PHASE 3: DIAMOND VISUAL (TQDM + LOGS)        ║")
print("╚════════════════════════════════════════════════════╝")

import os
import json
import time
import shutil
import zipfile
import concurrent.futures
import random
import re
import threading
import io
from datetime import timedelta

# External Libs
from tqdm import tqdm  # ✅ FOR PROGRESS BARS
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

# Automation Settings (Matching Reference Code)
MIN_QUESTIONS_TARGET = 5
MAX_QUESTIONS_TARGET = 50 

TOTAL_WORKERS = int(os.environ.get("TOTAL_WORKERS", 1)) 
WORKER_ID = int(os.environ.get("WORKER_ID", 1))

# --- 🟢 TELEGRAM TERMINAL (Silent Mode for Console) ---
class TelegramTerminal:
    def __init__(self):
        self.token = os.environ.get("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.environ.get("TELEGRAM_CHAT_ID")
        self.last_update_time = 0

    def send(self, text):
        """Sends message to Telegram ONLY (Doesn't print to console to avoid tqdm conflict)"""
        if not self.token: return
        try:
            import urllib.request, urllib.parse
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            data = urllib.parse.urlencode({"chat_id": self.chat_id, "text": text, "parse_mode": "HTML"}).encode()
            urllib.request.urlopen(urllib.request.Request(url, data=data))
        except: pass

telegram = TelegramTerminal()

# --- SETUP ---
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

# --- API KEY MANAGEMENT ---
api_keys = json.loads(os.environ.get("GEMINI_API_KEYS_LIST", "[]"))
if not api_keys: exit("❌ No API Keys")

key_lock = threading.Lock()
current_key_index = (WORKER_ID - 1) % len(api_keys)

def get_next_key():
    global current_key_index
    with key_lock:
        current_key_index = (current_key_index + 1) % len(api_keys)
        return api_keys[current_key_index]

def get_current_key():
    with key_lock: return api_keys[current_key_index]

# --- MEMORY RECALL ---
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
        return True
    except: return False

# --- GEMINI CALLER (GENERIC) ---
def call_gemini(img_path, prompt, task_type="Generation"):
    """Generic function to call Gemini with retry logic"""
    max_key_tries = len(api_keys)
    
    for _ in range(max_key_tries):
        try:
            # Random Sleep for Rate Limit Safety
            time.sleep(random.uniform(2, 5))
            
            genai.configure(api_key=get_current_key())
            model = genai.GenerativeModel('gemini-2.5-flash')
            img_file = genai.upload_file(img_path)
            
            wait = 0
            while img_file.state.name == "PROCESSING":
                time.sleep(1)
                img_file = genai.get_file(img_file.name)
                wait += 1
                if wait > 20: raise TimeoutError("Processing Timeout")

            response = model.generate_content([prompt, img_file])
            return response.text
            
        except google_exceptions.ResourceExhausted:
            get_next_key() # Switch key
            time.sleep(5)
        except Exception as e:
            if "429" in str(e):
                get_next_key()
                time.sleep(10)
            else:
                # print(f"\n⚠️ {task_type} Error: {e}") # Silent error to not break tqdm
                return None
    return None

# --- STEP 1: ANALYZE IMAGE ---
def analyze_image(img_path):
    prompt = "Analyze this image and estimate the total number of unique, high-quality MCQs possible. Provide only a single integer number as your answer."
    response = call_gemini(img_path, prompt, "Analysis")
    try:
        return int(response.strip())
    except:
        return 10 # Default fallback

# --- STEP 2: GENERATE MCQS ---
def generate_questions(img_path, target_count, master_prompt):
    final_prompt = f"{master_prompt}\n\nCreate exactly {target_count} unique MCQs from this image."
    response = call_gemini(img_path, final_prompt, "Generation")
    if not response: return None
    
    clean_text = response.replace("```json", "").replace("```", "").strip()
    if clean_text.startswith("[") or clean_text.startswith("{"):
        return clean_text
    return None

# --- ZIP & SYNC ---
def sync_chapter(drive_service, chapter_name, zip_path):
    if not drive_service: return
    try:
        filename = os.path.basename(zip_path)
        # Delete Old
        q = f"name = '{filename}' and trashed = false"
        res = drive_service.files().list(q=q, fields="files(id)").execute()
        for f in res.get('files', []):
            try: drive_service.files().delete(fileId=f['id']).execute()
            except: pass
            
        # Upload New
        meta = {'name': filename}
        media = MediaFileUpload(zip_path, mimetype='application/zip')
        file = drive_service.files().create(body=meta, media_body=media, fields='id').execute()
        
        # Notify Firebase
        drive_service.permissions().create(fileId=file['id'], body={'type': 'anyone', 'role': 'reader'}).execute()
        link = f"https://drive.google.com/uc?export=download&id={file['id']}"
        ref = db.reference('updates/latest')
        ref.set({"version": int(time.time()), "url": link, "message": f"Updated: {chapter_name}"})
        telegram.send(f"✅ <b>Synced:</b> {chapter_name}")
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
    
    telegram.send(f"🏭 <b>Worker {WORKER_ID} Started</b>")

    # 1. Find Chapters
    all_chapters = []
    for root, dirs, files in os.walk(INPUT_DIR):
        if any(f.endswith('.jpg') for f in files):
            all_chapters.append(root)
    all_chapters.sort()

    if not all_chapters:
        print("❌ No chapters found.")
        return

    # 2. Matrix Assign
    my_chapters = []
    for i, chap in enumerate(all_chapters):
        if (i % TOTAL_WORKERS) == (WORKER_ID - 1):
            my_chapters.append(chap)

    print(f"📚 Total Chapters: {len(all_chapters)}")
    print(f"🔧 My Assignment: {len(my_chapters)} chapters (Worker {WORKER_ID})")
    print("-" * 50)

    # 3. Main Loop (TQDM for Chapters)
    chapter_pbar = tqdm(my_chapters, desc="Processing Chapters", unit="chap")
    
    for chapter_path in chapter_pbar:
        chapter_name = os.path.basename(chapter_path)
        rel_chap_path = os.path.relpath(chapter_path, INPUT_DIR)
        
        # Update Description
        chapter_pbar.set_description(f"📂 {chapter_name}")
        
        # Restore
        download_previous_work(drive, chapter_name)
        
        images = sorted([os.path.join(chapter_path, f) for f in os.listdir(chapter_path) if f.endswith('.jpg')])
        
        # Inner Loop (TQDM for Images within Chapter)
        for img in tqdm(images, desc=f"  ↳ Images", unit="img", leave=False):
            img_name = os.path.basename(img)
            rel_img_path = os.path.relpath(img, INPUT_DIR) # e.g. Unit1/Chap1/1.jpg
            json_out = os.path.join(OUTPUT_DIR, rel_img_path).replace(".jpg", ".json")
            
            # Skip if exists
            if os.path.exists(json_out):
                tqdm.write(f"    ⏭️ Skipping {rel_img_path} (Exists)")
                continue
                
            os.makedirs(os.path.dirname(json_out), exist_ok=True)
            
            # --- STEP A: ANALYZE ---
            tqdm.write(f"    🔍 Analyzing {rel_img_path}...")
            est_count = analyze_image(img)
            
            target = min(MAX_QUESTIONS_TARGET, max(MIN_QUESTIONS_TARGET, est_count))
            tqdm.write(f"      > Analysis suggested: {est_count}, Target set to: {target}")
            
            # --- STEP B: GENERATE ---
            result = generate_questions(img, target, master_prompt)
            
            if result:
                with open(json_out, 'w', encoding='utf-8') as f: f.write(result)
                # tqdm.write(f"      ✅ Generated JSON for {img_name}")
            else:
                tqdm.write(f"      ❌ Failed to generate: {img_name}")
        
        # Sync after chapter
        zp = zip_chapter_local(chapter_path)
        if zp: sync_chapter(drive, chapter_name, zp)

    print("\n✅ WORKER COMPLETE")
    telegram.send(f"✅ <b>Worker {WORKER_ID} Finished.</b>")

if __name__ == "__main__":
    main()
