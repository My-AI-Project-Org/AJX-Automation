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
# Top imports ke saath ye jod lein
from google.api_core.exceptions import ResourceExhausted

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
    print(f"{icons.get(level, '')} [{level}] {msg}",flush=True)

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
        self.initial_count = len(self.my_keys)
        
        log("ORACLE", f"Worker {shard_index+1}/{total_shards} loaded {len(self.my_keys)} API Keys.")


    def get_random_key(self):
        """Rotates keys to avoid Rate Limits"""
        return random.choice(self.my_keys)

    def process_chapter(self, folder_name, folder_id, meta_data):
        log("ORACLE", f"📂 Scanning Chapter: {folder_name}")
        
        # 1. Audit Files (Saari files ki list lo)
        files = list_files_in_folder(folder_id)
        file_map = {f['name']: f['id'] for f in files}
        
        # Create Case-Insensitive Map (Comparison ke liye)
        existing_files_upper = {f['name'].upper(): f['id'] for f in files}

        # =====================================================
        # 🔥 NAYA LOGIC: METHOD 2 BYPASS (DIRECT TEXT TO MCQ)
        # =====================================================
        if meta_data.get('mode') == "METHOD_2":
            if "1.JSON" in existing_files_upper:
                log("SKIP", f"⏭️ {folder_name} is already generated (Method 2).")
                return

            log("GEMINI", f"✨ Processing Direct Text Generation for {folder_name}...")
            
            target_mcqs = meta_data.get("target_mcqs", 50)
            context = meta_data.get("subtopic_context", "")
            
            dynamic_prompt = (
                f"{meta_data.get('master_prompt', 'Generate MCQs in JSON format.')}\n\n"
                f"--- CONTEXT INFO ---\n"
                f"Subject: {meta_data['subject_key']}\n"
                f"Chapter: {folder_name}\n"
                f"Subtopic Details to Cover: {context}\n"
                f"Target Number of MCQs: {target_mcqs}\n"
                f"IMPORTANT: Generate exactly {target_mcqs} questions based ONLY on the subtopic details. Return ONLY valid JSON."
            )
            
            retries = 0
            while retries < MAX_RETRIES:
                if not self.my_keys:
                    log("CRITICAL", "❌ All API Keys exhausted for this worker!")
                    break

                current_api_key = self.get_random_key()
                try:
                    genai.configure(api_key=current_api_key)
                    # 🔥 NAYA: Advanced Generation Config (Tokens & Strictness)
                    generation_config = {
                        "temperature": 0.4, 
                        "top_p": 1, 
                        "top_k": 1, 
                        "max_output_tokens": 16384
                    }
                    model = genai.GenerativeModel('gemini-2.5-flash', generation_config=generation_config)
                    
                    # 🔥 No Image, Only Prompt passed to Gemini
                    response = model.generate_content(dynamic_prompt)
                    data = recursive_repair(response.text)
                    
                    if data:
                        for idx, q in enumerate(data):
                            q['local_id'] = idx + 1
                            q['source_image'] = "SYLLABUS_DB" # Method 2 indicator
                        
                        if upload_json(data, "1.json", folder_id):
                            log("SUCCESS", f"✅ Generated 1.json (Contains {len(data)} MCQs)")
                            return # ✅ Kaam ho gaya, return kar jao
                    else:
                        raise Exception("Gemini returned invalid JSON")

                except ResourceExhausted:
                    log("WARNING", f"⚠️ Quota Exceeded for key ...{current_api_key[-5:]}. Removing from pool.")
                    if current_api_key in self.my_keys: self.my_keys.remove(current_api_key)
                    remaining = len(self.my_keys)
                    used = self.initial_count - remaining
                    log("ACCOUNTANT", f"📉 STATUS: Used {used} | Remaining {remaining}")
                    continue 

                except Exception as e:
                    log("WARNING", f"Retry {retries+1}/{MAX_RETRIES} for Method 2: {e}")
                    retries += 1
                    time.sleep(5)
            
            log("ERROR", f"❌ Failed to process Method 2 for {folder_name}.")
            return

        # -----------------------------------------------------
        # 🕵️‍♂️ STEP 1: SMART AUDIT (Double Verification)
        # -----------------------------------------------------
        pending_images = []
        for name, fid in file_map.items():
            # Check karo agar ye Image hai
            if name.lower().endswith(('.jpg', '.jpeg', '.png')):
                
                # Expected JSON ka naam banao (e.g., "1.jpg" -> "1.json")
                base_name = name.rsplit('.', 1)[0]
                expected_json_name = f"{base_name}.json"
                
                # Check karo: Kya iska JSON exist karta hai? (Case Insensitive)
                if expected_json_name.upper() in existing_files_upper:
                    continue # ✅ Han hai, is image ko Skip karo.
                else:
                    # ❌ Nahi hai, isko list me daalo processing ke liye.
                    pending_images.append({'name': name, 'id': fid})

        # Sort Logic (Taaki sequence 1, 2, 3... rahe)
        try: pending_images.sort(key=lambda x: int(re.search(r'\d+', x['name']).group()))
        except: pending_images.sort(key=lambda x: x['name'])

        # -----------------------------------------------------
        # 🗑️ STEP 2: CORRUPT DATA CLEANUP (Self-Healing)
        # -----------------------------------------------------
        if not pending_images:
            # Case A: Sab kuch complete hai
            log("SKIP", f"⏭️ {folder_name} is 100% Complete. No missing JSONs.")
            return
        else:
            # Case B: Kuch JSONs missing hain
            log("INFO", f"⚙️ Found {len(pending_images)} missing JSONs in {folder_name}. Processing...")
            
            # Agar parts missing hain, par 'DATA.JSON' (Final file) wahan padi hai,
            # toh wo DATA.JSON jhootha/adhura hai. Usse DELETE karo.
            data_json_id = existing_files_upper.get("DATA.JSON")
            if data_json_id:
                log("DELETE", f"🗑️ Incomplete Folder detected! Deleting invalid DATA.JSON for {folder_name}...")
                try:
                    service.files().delete(fileId=data_json_id).execute()
                except Exception as e:
                    log("WARNING", f"Could not delete DATA.JSON: {e}")

        # -----------------------------------------------------
        # 🛠️ STEP 3: EXECUTION LOOP (Images Processing)
        # -----------------------------------------------------
        for img_item in pending_images:
            # ... (Iske niche ka code same rahega) ...
            img_name = img_item['name']
            img_id = img_item['id']
            json_target = img_name.rsplit('.', 1)[0] + ".json"
            
            try: page_num = int(re.search(r'\d+', img_name).group())
            except: page_num = 1
            
            # ... (Yahan se aapka Gemini API call shuru hoga) ...

            log("GEMINI", f"✨ Processing {img_name}...")
            
            # A. Download Image locally
            if not download_file(img_id, img_name): continue
            
            retries = 0
            success = False
            
            while retries < MAX_RETRIES:
                # 🔥 FIX: Agar keys khatam ho gayi to loop tod do
                if not self.my_keys:
                    log("CRITICAL", "❌ All API Keys exhausted for this worker!")
                    break

                # 🔥 FIX: Current Key ko variable me lo taaki remove kar sako
                current_api_key = self.get_random_key()

                try:
                    # Configure Gemini with specific key
                    genai.configure(api_key=current_api_key)
                    # 🔥 NAYA: Advanced Generation Config (Tokens & Strictness)
                    generation_config = {
                        "temperature": 0.4, 
                        "top_p": 1, 
                        "top_k": 1, 
                        "max_output_tokens": 16384
                    }
                    model = genai.GenerativeModel('gemini-2.5-flash', generation_config=generation_config)
                    
                    # Upload to Gemini (Temp)
                    sample_file = genai.upload_file(path=img_name, display_name=img_name)
                    
                    # Context Injection
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
                        # Assign IDs & Save
                        for idx, q in enumerate(data):
                            q['local_id'] = idx + 1
                            q['source_image'] = img_name
                        
                        if upload_json(data, json_target, folder_id):
                            log("SUCCESS", f"✅ Generated {json_target} (Contains {len(data)} MCQs)")
                            success = True
                            break # Kaam ho gaya, loop todo
                    else:
                        raise Exception("Gemini returned invalid/unrepairable JSON")

                # 🔥 SMART ROTATION CATCH BLOCK
                except ResourceExhausted:
                    log("WARNING", f"⚠️ Quota Exceeded for key ...{current_api_key[-5:]}. Removing from pool.")
                    # Is key ko list se nikaal do taaki dobara select na ho
                    if current_api_key in self.my_keys:
                        self.my_keys.remove(current_api_key)
                    # Retry count mat badhao, turant nayi key try karo
                    remaining = len(self.my_keys)
                    used = self.initial_count - remaining
                    # 📊 PRO STATUS BAR
                    log("ACCOUNTANT", f"📉 STATUS: Used {used} | Remaining {remaining} | Alive: {int((remaining/self.initial_count)*100)}%")
                    continue 

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
            # 🔥 NAYA LOGIC: Blueprint se 'mode' nikaalo (e.g. METHOD_1 or METHOD_2)
            method_type = data['meta'].get('mode', 'METHOD_1') 

            # =========================================================
            # 🔥 NAYA LOGIC: Dynamic Path for MASTER_PROMPT.txt
            # =========================================================
            custom_prompt = "Generate MCQs from this content in JSON format." # Default
            input_root_id = find_file_by_name("00_Input")
            if input_root_id:
                subject_input_folder_id = find_file_by_name(subject_key, parent_id=input_root_id)
                if subject_input_folder_id:
                    files_in_input = list_files_in_folder(subject_input_folder_id)
                    prompt_file_id = next((f['id'] for f in files_in_input if f['name'].upper() == "MASTER_PROMPT.TXT"), None)
                    
                    if prompt_file_id:
                        download_file(prompt_file_id, "temp_prompt.txt")
                        try:
                            with open("temp_prompt.txt", "r", encoding="utf-8") as f:
                                custom_prompt = f.read()
                            log("SUCCESS", f"📜 Master Prompt Loaded directly from 00_Input/{subject_key}")
                        except Exception as e:
                            log("WARNING", f"⚠️ Failed to read prompt: {e}")
                        finally:
                            if os.path.exists("temp_prompt.txt"): os.remove("temp_prompt.txt")
                    else:
                        log("WARNING", f"⚠️ MASTER_PROMPT.txt not found in 00_Input/{subject_key}. Using default.")
            
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
                    # Pass extra context needed for Prompt and Logic
                    chap['subject_key'] = subject_key
                    chap['mode'] = method_type  # 🔥 Method 1 ya 2 batane ke liye
                    chap['master_prompt'] = custom_prompt  # 🔥 NAYA: Worker ko prompt pass kar diya
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
            # Cleanup Blueprint (Taaki agla subject fresh start kare)
            if os.path.exists("blueprint.json"):
                os.remove("blueprint.json")

# ==========================================
# 🚀 ENTRY POINT
# ==========================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--shard", type=int, default=0)
    parser.add_argument("--total", type=int, default=1)
    args = parser.parse_args()

    oracle = AJXOracle(shard_index=args.shard, total_shards=args.total)
    oracle.execute()
