import os
import json
import time
import shutil
import zipfile
import concurrent.futures
from pathlib import Path
import random

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

# --- SETUP ---
console = Console()

# 1. SETUP FIREBASE & DRIVE
def init_services():
    # Drive Setup
    oauth_json = os.environ.get("GDRIVE_OAUTH_JSON")
    token_info = json.loads(oauth_json)
    creds = Credentials.from_authorized_user_info(token_info, ['https://www.googleapis.com/auth/drive'])
    drive_service = build('drive', 'v3', credentials=creds)

    # Firebase Setup (Check if already initialized to avoid error)
    if not firebase_admin._apps:
        fb_json = os.environ.get("FIREBASE_SERVICE_ACCOUNT")
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
    console.print(f"[bold green]🔔 Notification Sent for {chapter_name}![/bold green]")

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

# --- MAIN ORCHESTRATOR ---
def process_chapter(chapter_path, drive_service):
    chapter_name = os.path.basename(chapter_path)
    console.print(f"\n[bold yellow]📂 Starting Chapter: {chapter_name}[/bold yellow]")
    
    # 1. Find Images
    images = sorted([os.path.join(chapter_path, f) for f in os.listdir(chapter_path) if f.endswith('.jpg')])
    if not images: return
    
    # 2. Process in Parallel (Workers)
    # We use ThreadPoolExecutor to run 5-8 pages at once
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = {}
        for img in images:
            # Prepare Output Path
            rel_path = os.path.relpath(img, INPUT_DIR)
            json_out_path = os.path.join(OUTPUT_DIR, rel_path).replace(".jpg", ".json")
            
            # Check if done
            if os.path.exists(json_out_path):
                console.print(f"[dim]  Skipping {os.path.basename(img)} (Done)[/dim]")
                continue
                
            os.makedirs(os.path.dirname(json_out_path), exist_ok=True)
            
            # Submit Task
            prompt = """You are an Exam Expert. Extract MCQs. Output JSON list.""" # (Use your full prompt here)
            futures[executor.submit(generate_mcq, img, prompt)] = json_out_path
            
        # Wait for completion
        for future in concurrent.futures.as_completed(futures):
            json_path = futures[future]
            result = future.result()
            
            if result:
                with open(json_path, 'w', encoding='utf-8') as f:
                    f.write(result)
                console.print(f"  ✅ Generated: {os.path.basename(json_path)}")
            else:
                console.print(f"  ❌ Failed: {os.path.basename(json_path)}")

    # 3. Chapter Done? Pack & Ship!
    console.print(f"[cyan]📦 Packing Chapter: {chapter_name}...[/cyan]")
    zip_path = zip_chapter(chapter_path, INPUT_DIR)
    
    if zip_path:
        console.print(f"[cyan]🚀 Uploading & Notifying...[/cyan]")
        send_notification(drive_service, chapter_name, zip_path)
        # Optional: Sleep briefly to ensure Firebase updates don't overlap too fast
        time.sleep(5)

def main():
    os.makedirs(ZIP_DIR, exist_ok=True)
    drive_service = init_services()
    
    # 1. Identify Hierarchy
    # We want to iterate Chapter by Chapter.
    # Structure: Input / Exam_Subject / Unit / Chapter
    
    # Find the Exam Folder first
    exam_folders = [f for f in os.listdir(INPUT_DIR) if os.path.isdir(os.path.join(INPUT_DIR, f))]
    if not exam_folders:
        console.print("[red]No Input Data Found![/red]")
        return
        
    exam_root = os.path.join(INPUT_DIR, exam_folders[0]) # e.g., AJX_Phase1_Output/UPSI_History
    
    # Walk to find bottom-level folders (Chapters)
    # We assume any folder containing JPGs is a Chapter
    all_chapters = []
    for root, dirs, files in os.walk(exam_root):
        has_images = any(f.endswith('.jpg') for f in files)
        if has_images:
            all_chapters.append(root)
            
    all_chapters.sort() # Ensure order (Chapter 1, then 2...)
    
    console.print(f"[bold]Found {len(all_chapters)} Chapters to Process.[/bold]")
    
    # 2. Loop Through Chapters
    for chapter_path in all_chapters:
        process_chapter(chapter_path, drive_service)
        
    console.print("\n[bold green]🎉 BOOK COMPLETE![/bold green]")

if __name__ == "__main__":
    main()
