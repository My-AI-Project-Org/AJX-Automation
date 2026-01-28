import os
import json
import time
import random
import glob
import argparse
from pathlib import Path

# External Libraries
import google.generativeai as genai
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeRemainingColumn
from rich.panel import Panel

# Google Drive Libraries (ADDED FOR INSTANT SYNC)
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# --- SETUP ---
console = Console()

# Arguments
parser = argparse.ArgumentParser()
parser.add_argument('--worker_id', type=int, default=1)
parser.add_argument('--total_workers', type=int, default=1)
parser.add_argument('--config_path', type=str, required=True)
args = parser.parse_args()

# Load Config
with open(args.config_path, 'r') as f:
    full_config = json.load(f)

# Config Key nikalna (Kyuki JSON structure { "Exam_Config": {...} } hai)
config_key = list(full_config.keys())[0]
config = full_config[config_key]

INPUT_DIR = config['input_folder']
OUTPUT_DIR = "AJX_Worker_Output" # Local temp storage
BACKUP_DRIVE_FOLDER = "AJX_Phase4_Backup" # Drive destination

# --- DRIVE AUTHENTICATION (NEW) ---
drive_service = None

def init_drive():
    global drive_service
    oauth_json = os.environ.get("GDRIVE_OAUTH_JSON")
    if not oauth_json:
        console.print("[red]❌ Drive Token Missing! Real-time sync disabled.[/red]")
        return None
    
    try:
        token_info = json.loads(oauth_json)
        creds = Credentials.from_authorized_user_info(token_info, ['https://www.googleapis.com/auth/drive'])
        drive_service = build('drive', 'v3', credentials=creds)
        return drive_service
    except Exception as e:
        console.print(f"[red]❌ Drive Auth Failed: {e}[/red]")
        return None

# --- DRIVE UTILS (NEW) ---
def get_or_create_folder(folder_name, parent_id=None):
    if not drive_service: return None
    query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    if parent_id: query += f" and '{parent_id}' in parents"
    
    results = drive_service.files().list(q=query, fields="files(id)").execute()
    files = results.get('files', [])
    
    if files:
        return files[0]['id']
    else:
        meta = {'name': folder_name, 'mimeType': 'application/vnd.google-apps.folder'}
        if parent_id: meta['parents'] = [parent_id]
        folder = drive_service.files().create(body=meta, fields='id').execute()
        return folder.get('id')

def check_file_on_drive(filename, parent_id):
    """Checks if a JSON file already exists on Drive (For Resume Capability)"""
    if not drive_service or not parent_id: return False
    query = f"name = '{filename}' and '{parent_id}' in parents and trashed = false"
    results = drive_service.files().list(q=query, fields="files(id)").execute()
    return len(results.get('files', [])) > 0

def upload_to_drive_instant(filepath, filename, parent_id):
    """Uploads file immediately to Drive"""
    if not drive_service or not parent_id: return
    try:
        file_meta = {'name': filename, 'parents': [parent_id]}
        media = MediaFileUpload(filepath, mimetype='application/json')
        drive_service.files().create(body=file_meta, media_body=media).execute()
        # console.print(f"[green]☁️ Synced to Cloud: {filename}[/green]")
    except Exception as e:
        console.print(f"[red]⚠️ Cloud Sync Failed for {filename}: {e}[/red]")

# --- GEMINI SETUP ---
api_keys = json.loads(os.environ.get("GEMINI_API_KEYS_LIST", "[]"))
if not isinstance(api_keys, list): api_keys = [api_keys]
current_key_index = args.worker_id % len(api_keys) # Simple distribution

def get_model():
    global current_key_index
    key = api_keys[current_key_index]
    genai.configure(api_key=key)
    return genai.GenerativeModel('gemini-2.5-flash')

def rotate_key():
    global current_key_index
    current_key_index = (current_key_index + 1) % len(api_keys)
    get_model()

# --- MAIN WORKER LOGIC ---
def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # 1. Initialize Drive
    init_drive()
    
    # 2. Setup Backup Folder Hierarchy (Exam_Subject -> Chapter)
    # Structure: AJX_Phase4_Backup / Exam_Subject / ...
    root_backup_id = get_or_create_folder(BACKUP_DRIVE_FOLDER)
    exam_folder_name = f"{config['exam_name']}_{config['subject']}"
    exam_backup_id = get_or_create_folder(exam_folder_name, root_backup_id)

    # 3. Collect Images
    all_images = []
    # Recursively find all images in Phase 1 output
    for root, dirs, files in os.walk(INPUT_DIR):
        for file in files:
            if file.lower().endswith(('.jpg', '.jpeg', '.png')):
                all_images.append(os.path.join(root, file))
    
    all_images.sort()
    
    # 4. Distribute Work
    my_images = [img for i, img in enumerate(all_images) if i % args.total_workers == args.worker_id - 1]
    
    console.print(Panel(f"Worker {args.worker_id}/{args.total_workers} started.\nAssigned: {len(my_images)} images.", title="🚀 AJX Factory", style="bold blue"))

    model = get_model()
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeRemainingColumn()
    ) as progress:
        
        task = progress.add_task(f"[cyan]Worker {args.worker_id} Processing...", total=len(my_images))
        
        for img_path in my_images:
            # Prepare Paths
            relative_path = os.path.relpath(img_path, INPUT_DIR) # e.g., 01_Unit/01_Chapter/1.jpg
            folder_structure = os.path.dirname(relative_path)    # e.g., 01_Unit/01_Chapter
            filename = os.path.basename(img_path)
            json_filename = filename.replace(Path(filename).suffix, ".json")
            
            # --- RESUME CHECK (Cloud) ---
            # Drive par folder dhoondo
            path_parts = folder_structure.split(os.sep)
            current_parent_id = exam_backup_id
            
            # Navigate/Create folders in Drive to match local structure
            for part in path_parts:
                current_parent_id = get_or_create_folder(part, current_parent_id)
            
            # Check if JSON exists
            if check_file_on_drive(json_filename, current_parent_id):
                progress.console.print(f"[yellow]⏩ Skipping {json_filename} (Already in Cloud)[/yellow]")
                progress.advance(task)
                continue

            # --- GENERATION ---
            try:
                # console.print(f"Processing: {json_filename}")
                img_file = genai.upload_file(img_path)
                while img_file.state.name == "PROCESSING":
                    time.sleep(1)
                    img_file = genai.get_file(img_file.name)

                prompt = config['prompt_template'].format(
                    exam_name=config['exam_name'],
                    target_count=random.randint(config['min_q'], config['max_q']),
                    start_id=int(time.time()) # Unique ID based on time
                )

                response = model.generate_content([prompt, img_file])
                
                # Cleanup Text
                json_text = response.text.replace("```json", "").replace("```", "").strip()
                
                # Validation
                try:
                    data = json.loads(json_text)
                    if not isinstance(data, list): raise ValueError("Not a list")
                except:
                    # Retry logic simple
                    rotate_key()
                    response = model.generate_content([prompt, img_file])
                    json_text = response.text.replace("```json", "").replace("```", "").strip()

                # --- SAVE LOCAL (For Phase 4 Zip) ---
                local_save_dir = os.path.join(OUTPUT_DIR, folder_structure)
                os.makedirs(local_save_dir, exist_ok=True)
                local_path = os.path.join(local_save_dir, json_filename)
                
                with open(local_path, 'w', encoding='utf-8') as f:
                    f.write(json_text)
                
                # --- INSTANT UPLOAD (The Magic) ---
                upload_to_drive_instant(local_path, json_filename, current_parent_id)
                
                progress.advance(task)

            except Exception as e:
                console.print(f"[red]❌ Error on {filename}: {e}[/red]")
                rotate_key()
                # Fail hua to skip karo, retry agle run me hoga

if __name__ == "__main__":
    main()
