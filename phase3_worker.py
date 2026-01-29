print("╔════════════════════════════════════════════════════╗")
print("║   AJX PHASE 3: FACTORY WORKER (LIVE TELEGRAM)      ║")
print("╚════════════════════════════════════════════════════╝")

import os
import json
import time
import shutil
import zipfile
import concurrent.futures
from pathlib import Path
import random
import urllib.request
import urllib.parse
from collections import deque

# External Libs
import google.generativeai as genai
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import firebase_admin
from firebase_admin import credentials, db

# --- CONFIG ---
INPUT_DIR = "AJX_Phase1_Output"  # Local Folder with Images
OUTPUT_DIR = "AJX_Worker_Output" # Where JSONs are saved
ZIP_DIR = "AJX_Ready_Packages"   # Where we store zips
PROMPT_FILE_NAME = 'master_prompt.txt'

# --- 🟢 LIVE TELEGRAM TERMINAL SYSTEM ---
class TelegramTerminal:
    def __init__(self):
        self.token = os.environ.get("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.environ.get("TELEGRAM_CHAT_ID")
        self.message_id = None
        self.last_update_time = 0
        self.log_buffer = deque(maxlen=10) # Last 10 lines only
        self.current_progress = 0
        self.current_status = "Initializing Factory..."

    def start(self):
        if not self.token: return
        self.message_id = self._send_new("<b>🏭 AJX PHASE 3 (FACTORY)</b>\nStarting Workers...")

    def log_stream(self, msg):
        clean_msg = str(msg).replace("<", "&lt;").replace(">", "&gt;") 
        self.log_buffer.append(f"> {clean_msg}")
        self._refresh_display()

    def update_progress(self, percent, status):
        self.current_progress = percent
        self.current_status = status
        self._refresh_display()

    def _refresh_display(self):
        # Throttle updates (1.5 sec gap)
        if time.time() - self.last_update_time < 1.5 and self.current_progress < 100: return
        if not self.token or not self.message_id: return
        
        logs_text = "\n".join(self.log_buffer)
        bar_len = 10
        filled = int(bar_len * self.current_progress / 100)
        bar = "█" * filled + "░" * (bar_len - filled)
        
        text = (f"<b>🏭 AJX PHASE 3 (FACTORY)</b>\n<code>{logs_text}</code>\n"
                f"━━━━━━━━━━━━━━━━━━\n<b>{self.current_status}</b>\n"
                f"<code>[{bar}] {self.current_progress}%</code>")
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

# Initialize Terminal
terminal = TelegramTerminal()
console = Console()

# Custom Log Function
def log(msg):
    console.print(msg)
    terminal.log_stream(msg)

# --- SETUP ---

# 1. SETUP FIREBASE & DRIVE
def init_services():
    terminal.update_progress(5, "Connecting Cloud...")
    # Drive Setup
    oauth_json = os.environ.get("GDRIVE_OAUTH_JSON")
    if not oauth_json:
        log("❌ Secrets Missing: GDRIVE_OAUTH_JSON")
        return None
        
    token_info = json.loads(oauth_json)
    creds = Credentials.from_authorized_user_info(token_info, ['https://www.googleapis.com/auth/drive'])
    drive_service = build('drive', 'v3', credentials=creds)

    # Firebase Setup (Check if already initialized to avoid error)
    if not firebase_admin._apps:
        fb_json = os.environ.get("FIREBASE_SERVICE_ACCOUNT")
        if fb_json:
            cred = credentials.Certificate(json.loads(fb_json))
            firebase_admin.initialize_app(cred, {'databaseURL': os.environ.get("FIREBASE_DB_URL")})
    
    return drive_service

# 2. GEMINI SETUP (Key Rotation)
api_keys = json.loads(os.environ.get("GEMINI_API_KEYS_LIST", "[]"))
if not isinstance(api_keys, list): api_keys = [api_keys]

def generate_mcq(img_path, prompt):
    # Randomly pick a key to distribute load
    key = random.choice(api_keys)
    genai.configure(api_key=key)
    model = genai.GenerativeModel('gemini-2.5-flash')
    
    try:
        img_file = genai.upload_file(img_path)
        while img_file.state.name == "PROCESSING":
            time.sleep(1)
            img_file = genai.get_file(img_file.name)
            
        response = model.generate_content([prompt, img_file])
        return response.text.replace("```json", "").replace("```", "").strip()
    except Exception as e:
        return None # Return None on failure to retry

# 3. NOTIFICATION SYSTEM
def send_notification(drive_service, chapter_name, zip_path):
    terminal.update_progress(95, "Sending Notification...")
    # Upload Zip
    file_metadata = {'name': f"UPDATE_{chapter_name}_{int(time.time())}.zip"}
    media = MediaFileUpload(zip_path, mimetype='application/zip')
    file = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    
    # Public Link
    drive_service.permissions().create(fileId=file['id'], body={'type': 'anyone', 'role': 'reader'}).execute()
    link = f"https://drive.google.com/uc?export=download&id={file['id']}"
    
    # Firebase Trigger
    ref = db.reference('updates/latest')
    ref.set({
        "version": int(time.time()),
        "url": link,
        "message": f"New: {chapter_name} Added! 🚀"
    })
    log(f"🔔 Notification Sent for {chapter_name}!")

# 4. ZIPPER (Maintains Hierarchy)
def zip_chapter(chapter_path, relative_root):
    # relative_root is typically "AJX_Phase1_Output"
    # We need to preserve structure: Exam/Subject/Unit/Chapter
    
    chapter_name = os.path.basename(chapter_path)
    zip_filename = os.path.join(ZIP_DIR, f"{chapter_name}.zip")
    
    with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
        # Walk through the Output Directory corresponding to this chapter
        # We need to map Input Path -> Output Path
        
        # Calculate structure
        rel_path = os.path.relpath(chapter_path, INPUT_DIR) # e.g., UPSI_History/01_Unit/01_Chapter
        target_output_dir = os.path.join(OUTPUT_DIR, rel_path)
        
        if not os.path.exists(target_output_dir):
            return None # No JSONs generated?
            
        for root, _, files in os.walk(target_output_dir):
            for file in files:
                if file.endswith(".json"):
                    full_path = os.path.join(root, file)
                    # Zip Entry Name must match hierarchy: UPSI_History/01_Unit/01_Chapter/file.json
                    arcname = os.path.relpath(full_path, OUTPUT_DIR)
                    zipf.write(full_path, arcname)
                    
    return zip_filename

def get_prompt_content():
    """Smart Search for master_prompt.txt"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    paths = [
        os.path.join(script_dir, PROMPT_FILE_NAME),
        os.path.join(os.getcwd(), PROMPT_FILE_NAME),
        os.path.join(os.path.dirname(script_dir), PROMPT_FILE_NAME)
    ]
    for p in paths:
        if os.path.exists(p):
            try: return open(p, 'r', encoding='utf-8').read()
            except: pass
    return "Extract MCQs from this image in strict JSON format." # Fallback

# --- MAIN ORCHESTRATOR ---
def process_chapter(chapter_path, drive_service, master_prompt):
    chapter_name = os.path.basename(chapter_path)
    log(f"📂 Starting Chapter: {chapter_name}")
    terminal.update_progress(10, f"Processing: {chapter_name}")
    
    # 1. Find Images
    images = sorted([os.path.join(chapter_path, f) for f in os.listdir(chapter_path) if f.endswith('.jpg')])
    if not images: return
    
    total_imgs = len(images)
    
    # 2. Process in Parallel (Workers)
    # We use ThreadPoolExecutor to run 8 pages at once
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = {}
        for img in images:
            # Prepare Output Path
            rel_path = os.path.relpath(img, INPUT_DIR)
            json_out_path = os.path.join(OUTPUT_DIR, rel_path).replace(".jpg", ".json")
            
            # Check if done
            if os.path.exists(json_out_path):
                continue
                
            os.makedirs(os.path.dirname(json_out_path), exist_ok=True)
            
            # Submit Task
            futures[executor.submit(generate_mcq, img, master_prompt)] = json_out_path
            
        # Wait for completion
        completed_count = 0
        for future in concurrent.futures.as_completed(futures):
            json_path = futures[future]
            result = future.result()
            
            completed_count += 1
            # Update Progress Bar (10% to 90%)
            percent = 10 + int((completed_count / total_imgs) * 80)
            terminal.update_progress(percent, f"Gen: {os.path.basename(json_path)}")
            
            if result:
                with open(json_path, 'w', encoding='utf-8') as f:
                    f.write(result)
            else:
                log(f"❌ Failed: {os.path.basename(json_path)}")

    # 3. Chapter Done? Pack & Ship!
    terminal.update_progress(90, "Packing ZIP...")
    log(f"📦 Packing Chapter: {chapter_name}...")
    zip_path = zip_chapter(chapter_path, INPUT_DIR)
    
    if zip_path:
        log(f"🚀 Uploading & Notifying...")
        send_notification(drive_service, chapter_name, zip_path)
        # Optional: Sleep briefly to ensure Firebase updates don't overlap too fast
        time.sleep(5)

def main():
    terminal.start()
    os.makedirs(ZIP_DIR, exist_ok=True)
    
    drive_service = init_services()
    if not drive_service:
        terminal.update_progress(0, "CLOUD ERROR")
        return

    # Load Prompt
    master_prompt = get_prompt_content()
    
    # 1. Identify Hierarchy
    # We want to iterate Chapter by Chapter.
    # Structure: Input / Exam_Subject / Unit / Chapter
    
    # Find the Exam Folder first (Should be only 1 after Phase 1)
    # But wait, Phase 1 creates folders like '01_Unit_...' directly if no exam folder logic
    # Let's just walk and find any folder with JPGs
    
    all_chapters = []
    for root, dirs, files in os.walk(INPUT_DIR):
        has_images = any(f.endswith('.jpg') for f in files)
        if has_images:
            all_chapters.append(root)
            
    all_chapters.sort() # Ensure order (Chapter 1, then 2...)
    
    log(f"Found {len(all_chapters)} Chapters to Process.")
    
    # 2. Loop Through Chapters
    total_chapters = len(all_chapters)
    for i, chapter_path in enumerate(all_chapters):
        process_chapter(chapter_path, drive_service, master_prompt)
        
        # Chapter Complete Notification
        terminal.log_stream(f"✅ Chapter {i+1}/{total_chapters} Done")
        
    terminal.update_progress(100, "🎉 BOOK COMPLETE")
    log("🎉 BOOK COMPLETE!")

if __name__ == "__main__":
    main()
