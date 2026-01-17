import os
import io
import json
import time
import shutil
import google.generativeai as genai
from google.api_core import exceptions
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from pdf2image import convert_from_path

# --- CONFIGURATION ---
INPUT_FOLDER_NAME = 'AJX_Input'
OUTPUT_FOLDER_NAME = 'AJX_Phase1_Output'

# 1. LOAD ALL KEYS FROM SECRET (ROTATION LOGIC)
keys_json = os.environ.get("GEMINI_API_KEYS_LIST")
try:
    API_KEYS = json.loads(keys_json)
except:
    API_KEYS = [keys_json] # Fallback for single key

current_key_index = 0

def configure_genai():
    """Configures Gemini with the current active key"""
    global current_key_index
    active_key = API_KEYS[current_key_index]
    genai.configure(api_key=active_key)
    print(f"🔑 Using API Key #{current_key_index + 1}")

# Initial Configuration
configure_genai()
model = genai.GenerativeModel('gemini-1.5-flash')

# Setup Drive
creds_json = os.environ.get("GDRIVE_CREDENTIALS")
creds_dict = json.loads(creds_json)
creds = service_account.Credentials.from_service_account_info(
    creds_dict, scopes=['https://www.googleapis.com/auth/drive']
)
service = build('drive', 'v3', credentials=creds)

# --- ROTATION LOGIC ---
def generate_content_with_rotation(prompt_parts):
    global current_key_index
    max_retries = len(API_KEYS) 
    attempts = 0

    while attempts < max_retries:
        try:
            return model.generate_content(prompt_parts)
        except exceptions.ResourceExhausted:
            print(f"⚠️ Key #{current_key_index + 1} exhausted! Switching...")
            current_key_index = (current_key_index + 1) % len(API_KEYS)
            configure_genai()
            attempts += 1
            time.sleep(2)
        except Exception as e:
            print(f"❌ Other Error: {e}")
            return None
    print("❌ All API Keys are exhausted!")
    return None

# --- DRIVE FUNCTIONS ---
def get_folder_id(folder_name):
    query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    results = service.files().list(q=query, fields="files(id)").execute()
    files = results.get('files', [])
    return files[0]['id'] if files else None

def download_latest_pdf(folder_id):
    query = f"'{folder_id}' in parents and mimeType = 'application/pdf' and trashed = false"
    results = service.files().list(q=query, orderBy='createdTime desc', pageSize=1).execute()
    items = results.get('files', [])
    if not items: return None
    
    file_id = items[0]['id']
    file_name = items[0]['name']
    print(f"⬇️ Downloading: {file_name}")
    request = service.files().get_media(fileId=file_id)
    fh = io.FileIO(file_name, 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    return file_name

def get_smart_chapters(images):
    print("🧠 Analyzing Index with Key Rotation...")
    toc_images = images[:20]
    
    prompt = """
    Analyze these images. Find the Table of Contents.
    Identify the STARTING PDF PAGE INDEX (0-based) for each chapter.
    Return strictly a JSON list:
    [{"name": "00_Preface", "start_index": 0}, {"name": "01_Intro", "start_index": 12}]
    If no index found, return [].
    """
    
    response = generate_content_with_rotation([prompt] + toc_images)
    
    if response and response.text:
        try:
            clean_json = response.text.replace("```json", "").replace("```", "").strip()
            return json.loads(clean_json)
        except:
            return []
    return []

def organize_folders(images, chapters):
    base = "organized_output"
    if os.path.exists(base): shutil.rmtree(base)
    os.makedirs(base)

    if not chapters:
        folder = os.path.join(base, "Full_Book")
        os.makedirs(folder)
        for i, img in enumerate(images):
            img.save(f"{folder}/{str(i+1).zfill(3)}.jpg", "JPEG")
        return base

    print(f"📂 Sorting into {len(chapters)} chapters...")
    for i, chap in enumerate(chapters):
        safe_name = "".join(c for c in chap['name'] if c.isalnum() or c in (' ', '_')).strip()[:30]
        folder_name = f"{str(i+1).zfill(2)}_{safe_name}"
        folder_path = os.path.join(base, folder_name)
        os.makedirs(folder_path, exist_ok=True)
        
        start = int(chap['start_index'])
        if i < len(chapters) - 1:
            end = int(chapters[i+1]['start_index'])
        else:
            end = len(images)
            
        for p in range(start, end):
            if p < len(images):
                local_page_num = p - start + 1
                images[p].save(f"{folder_path}/{str(local_page_num).zfill(3)}.jpg", "JPEG")
    return base

def upload_to_drive(local_base, parent_id, pdf_name):
    book_meta = {'name': pdf_name.replace('.pdf', ''), 'mimeType': 'application/vnd.google-apps.folder', 'parents': [parent_id]}
    book_folder = service.files().create(body=book_meta, fields='id').execute()
    book_id = book_folder.get('id')

    for folder in sorted(os.listdir(local_base)):
        local_path = os.path.join(local_base, folder)
        if os.path.isdir(local_path):
            print(f"⬆️ Uploading: {folder}")
            chap_meta = {'name': folder, 'parents': [book_id], 'mimeType': 'application/vnd.google-apps.folder'}
            chap_drive = service.files().create(body=chap_meta, fields='id').execute()
            for img in sorted(os.listdir(local_path)):
                file_meta = {'name': img, 'parents': [chap_drive.get('id')]}
                media = MediaFileUpload(os.path.join(local_path, img), mimetype='image/jpeg')
                service.files().create(body=file_meta, media_body=media).execute()

def main():
    in_id = get_folder_id(INPUT_FOLDER_NAME)
    out_id = get_folder_id(OUTPUT_FOLDER_NAME)
    
    pdf = download_latest_pdf(in_id)
    if not pdf: 
        print("❌ No PDF found!")
        return

    print("⚙️ Converting PDF to Images...")
    images = convert_from_path(pdf)
    
    chapters = get_smart_chapters(images)
    organized_path = organize_folders(images, chapters)
    
    upload_to_drive(organized_path, out_id, pdf)
    print("🎉 Phase 1 Complete!")

if __name__ == "__main__":
    main()
