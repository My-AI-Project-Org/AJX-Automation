import os
import json
import time
import shutil
import re
import sys
import io
import random
import argparse
import google.generativeai as genai
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from googleapiclient.errors import HttpError

# ==========================================
# ⚙️ CONFIGURATION
# ==========================================
SCOPES = ['https://www.googleapis.com/auth/drive']
MAX_RETRIES = 3

def log(level, msg):
    """Clean, Icon-based Logger"""
    icons = {
        "INFO": "ℹ️", "SUCCESS": "✅", "WARNING": "⚠️", 
        "ERROR": "❌", "CRITICAL": "💀", "ORACLE": "🔮", 
        "SKIP": "⏭️", "GEMINI": "✨"
    }
    print(f"{icons.get(level, '')} [{level}] {msg}")

# ==========================================
# 🔐 AUTHENTICATION & DRIVE SETUP
# ==========================================
def setup_drive():
    """Authenticates using either OAuth (User) or Service Account (Robot)"""
    # 1. Try OAuth (Priority)
    oauth_json = os.environ.get("GDRIVE_OAUTH_JSON")
    if oauth_json:
        try:
            creds = Credentials.from_authorized_user_info(json.loads(oauth_json), SCOPES)
            return build('drive', 'v3', credentials=creds)
        except Exception as e:
            log("WARNING", f"OAuth Auth failed, trying Service Account... {e}")

    # 2. Try Service Account
    sa_json = os.environ.get("GDRIVE_CREDENTIALS")
    if sa_json:
        try:
            creds = service_account.Credentials.from_service_account_info(json.loads(sa_json), scopes=SCOPES)
            return build('drive', 'v3', credentials=creds)
        except Exception as e:
            log("CRITICAL", f"Service Account Auth failed: {e}")
    
    log("CRITICAL", "❌ No valid Google Drive Credentials found.")
    sys.exit(1)

service = setup_drive()

# ==========================================
# 📂 DRIVE HELPER FUNCTIONS
# ==========================================
def find_file_by_name(name, parent_id=None):
    """Finds a file/folder ID by name"""
    query = f"name = '{name}' and trashed = false"
    if parent_id: query += f" and '{parent_id}' in parents"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get('files', [])
    return files[0]['id'] if files else None

def list_files_in_folder(folder_id):
    """Lists all files in a folder (for Audit)"""
    query = f"'{folder_id}' in parents and trashed = false"
    results = service.files().list(q=query, fields="files(id, name)", pageSize=1000).execute()
    return results.get('files', [])

def download_file(file_id, local_path):
    """Downloads a file from Drive"""
    try:
        request = service.files().get_media(fileId=file_id)
        fh = io.FileIO(local_path, 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done: status, done = downloader.next_chunk()
        fh.close()
        return True
    except Exception as e:
        log("ERROR", f"Download failed for {local_path}: {e}")
        return False

def upload_json(data, filename, folder_id):
    """Uploads JSON result back to the same folder"""
    local_path = f"temp_{filename}"
    try:
        with open(local_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
        
        meta = {'name': filename, 'parents': [folder_id]}
        media = MediaFileUpload(local_path, mimetype='application/json')
        service.files().create(body=meta, media_body=media).execute()
        return True
    except Exception as e:
        log("ERROR", f"Upload failed for {filename}: {e}")
        return False
    finally:
        if os.path.exists(local_path): os.remove(local_path)

# ==========================================
# 🧠 JSON REPAIR (DSA LOGIC from Old Script)
# ==========================================
def recursive_repair(raw_text):
    """Extracts JSON from Markdown and repairs syntax errors"""
    try:
        # 1. Strip Markdown Code Blocks (```json ... ```)
        clean_text = re.sub(r'```json|```', '', raw_text).strip()
        
        # 2. Find Start/End Brackets to remove preamble text
        if "[" in clean_text: clean_text = "[" + clean_text.split("[", 1)[1]
        if "]" in clean_text: clean_text = clean_text.rsplit("]", 1)[0] + "]"
        
        return json.loads(clean_text)
    except:
        return None

# ==========================================
# 🔮 THE ORACLE ENGINE
# ==========================================
class AJXOracle:
    def __init__(self, shard_index, total_shards):
        self.shard_index = shard_index
        self.total_shards = total_shards
        
        # 1. Load & Distribute API Keys (Load Balancing)
        all_keys_str = os.environ.get("GEMINI_API_KEYS_LIST", "")
        if not all_keys_str:
            log("CRITICAL", "GEMINI_API_KEYS_LIST secrets missing!")
            sys.exit(1)
            
        all_keys = [k.strip() for k in all_keys_str.split(",") if k.strip()]
        
        # Logic: If 20 keys and 5 workers, Worker 0 gets keys 0-3
        chunk_size = len(all_keys) // total_shards if total_shards > 0 else 1
        start = shard_index * chunk_size
        end = start + chunk_size
        
        # Fallback: If math is weird, take all keys (better than crashing)
        self.my_keys = all_keys[start:end] if len(all_keys) >= total_shards else all_keys
        
        log("ORACLE", f"Worker {shard_index+1}/{total_shards} loaded {len(self.my_keys)} API Keys.")

        # 2. Fetch Master Prompt from Drive
        self.master_prompt = "Generate MCQs from this image in JSON format." # Default safety
        prompt_id = find_file_by_name("MASTER_PROMPT.txt")
        
        if prompt_id:
            download_file(prompt_id, "MASTER_PROMPT.txt")
            with open("MASTER_PROMPT.txt", "r", encoding="utf-8") as f:
                self.master_prompt = f.read()
            log("SUCCESS", "📜 Master Prompt Loaded from Drive.")
        else:
            log("WARNING", "⚠️ MASTER_PROMPT.txt not found in Root. Using Default.")

    def get_random_key(self):
        """Rotates keys to avoid Rate Limits"""
        return random.choice(self.my_keys)

    def process_chapter(self, folder_name, folder_id, meta_data):
        """Processes images in a specific chapter folder"""
        log("ORACLE", f"📂 Scanning Chapter: {folder_name}")
        
        # 1. Audit: List all files
        files = list_files_in_folder(folder_id)
        file_map = {f['name']: f['id'] for f in files}
        
        # 2. Identify Pending Work
        pending_images = []
        for name, fid in file_map.items():
            if name.endswith(".jpg"):
                json_name = name.replace(".jpg", ".json")
                
                # 🔥 SKIP LOGIC: If JSON exists, don't redo it
                if json_name in file_map: 
                    continue 
                
                pending_images.append({'name': name, 'id': fid})
        
        # Sort nicely (1.jpg, 2.jpg... 10.jpg)
        try: pending_images.sort(key=lambda x: int(re.search(r'\d+', x['name']).group()))
        except: pending_images.sort(key=lambda x: x['name'])

        if not pending_images:
            log("SKIP", f"⏭️ {folder_name} is Complete (All JSONs exist).")
            return

        log("INFO", f"⚙️ Found {len(pending_images)} pending images in {folder_name}")

        # 3. Process Loop
        for img_item in pending_images:
            img_name = img_item['name']
            img_id = img_item['id']
            json_target = img_name.replace(".jpg", ".json")
            
            # Derive Page Number for Context
            try: page_num = int(re.search(r'\d+', img_name).group())
            except: page_num = 1

            log("GEMINI", f"✨ Processing {img_name}...")
            
            # A. Download Image locally
            if not download_file(img_id, img_name): continue
            
            retries = 0
            success = False
            
            while retries < MAX_RETRIES:
                try:
                    # Configure Gemini
                    api_key = self.get_random_key()
                    genai.configure(api_key=api_key)
                    model = genai.GenerativeModel('gemini-2.0-flash')
                    
                    # Upload to Gemini (Temp)
                    sample_file = genai.upload_file(path=img_name, display_name=img_name)
                    
                    # 🔥 CONTEXT INJECTION
                    # We inject the specific Chapter Name and Page Number into the prompt
                    dynamic_prompt = (
                        f"{self.master_prompt}\n\n"
                        f"--- CONTEXT INFO ---\n"
                        f"Subject: {meta_data['subject_key']}\n"
                        f"Chapter: {folder_name}\n"
                        f"Part/Page Number: {page_num}\n"
                        f"IMPORTANT: Return ONLY valid JSON."
                    )
                    
                    # Generate
                    response = model.generate_content([dynamic_prompt, sample_file])
                    
                    # B. Repair & Validate
                    data = recursive_repair(response.text)
                    
                    if data:
                        # 🛠️ Assign Local IDs (1, 2, 3...) just for structure
                        # Global IDs will be handled by Phase 4 (Courier)
                        for idx, q in enumerate(data):
                            q['local_id'] = idx + 1
                            q['source_image'] = img_name
                        
                        # C. Upload Result
                        if upload_json(data, json_target, folder_id):
                            log("SUCCESS", f"✅ Generated {json_target}")
                            success = True
                            break
                    else:
                        raise Exception("Gemini returned invalid/unrepairable JSON")

                except Exception as e:
                    log("WARNING", f"Retry {retries+1}/{MAX_RETRIES} for {img_name}: {e}")
                    retries += 1
                    time.sleep(5) # Cooldown
            
            # Cleanup Local Image
            if os.path.exists(img_name): os.remove(img_name)
            
            if not success:
                log("ERROR", f"❌ Failed to process {img_name} after retries. Skipping.")
            
            # Polite Delay
            time.sleep(2)

    def execute(self):
        log("INFO", f"🚀 ORACLE WORKER STARTED (Shard {self.shard_index+1}/{self.total_shards})")
        
        # 1. Find Blueprints Folder
        bp_folder_id = find_file_by_name("01_Blueprints")
        if not bp_folder_id:
            log("WARNING", "01_Blueprints folder not found. Run Architect First.")
            return

        blueprints = list_files_in_folder(bp_folder_id)
        
        for bp in blueprints:
            if not bp['name'].endswith("_BLUEPRINT.json"): continue
            
            # Download & Read Blueprint
            download_file(bp['id'], "blueprint.json")
            with open("blueprint.json", "r") as f: data = json.load(f)
            
            subject_key = data['meta']['subject_key']
            
            # 2. Find Subject Folder in Masonry
            masonry_root = find_file_by_name("02_Masonry")
            subject_folder_id = find_file_by_name(subject_key, parent_id=masonry_root)
            
            if not subject_folder_id:
                log("WARNING", f"Masonry folder for {subject_key} not found. Run Mason First.")
                continue

            # 3. Flatten Chapters & Apply Sharding
            flat_chapters = []
            for unit in data['structure']:
                for chap in unit['chapters']:
                    # Pass extra context needed for Prompt
                    chap['subject_key'] = subject_key
                    flat_chapters.append(chap)
            
            # Optimization: Load Subject Folder Listing ONCE to find IDs
            all_folders = list_files_in_folder(subject_folder_id)
            folder_map = {f['name']: f['id'] for f in all_folders}

            # Loop through chapters
            for idx, chap in enumerate(flat_chapters):
                # 🔥 DEVOPS: SHARDING LOGIC
                if idx % self.total_shards != self.shard_index:
                    continue # Not my job, skip
                
                chap_id = chap['id']
                # Match folder starting with "ID_" (e.g. "101_Stone_Age")
                target_folder_name = next((name for name in folder_map if name.startswith(f"{chap_id}_")), None)
                
                if target_folder_name:
                    self.process_chapter(target_folder_name, folder_map[target_folder_name], chap)
                else:
                    log("WARNING", f"Chapter folder {chap_id} not found in Drive. Did Mason finish?")
            
            # Cleanup Blueprint
            if os.path.exists("blueprint.json
