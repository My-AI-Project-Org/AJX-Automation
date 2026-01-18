print("╔════════════════════════════════════════════╗")
print("║   AJX ULTIMATE: OAUTH USER MODE ACTIVE     ║")
print("╚════════════════════════════════════════════╝")

import os
import io
import json
import time
import shutil
import sys

# Prints flush immediately
def log(msg):
    print(msg)
    sys.stdout.flush()

log("🔍 STEP 1: Importing Modules...")
# 👇 THIS IS THE CHANGE: We import 'Credentials' for Users, not ServiceAccount
from google.oauth2.credentials import Credentials 
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
import google.generativeai as genai
from pdf2image import convert_from_path, pdfinfo_from_path

# --- CONFIGURATION ---
INPUT_FOLDER_NAME = 'AJX_Input'
OUTPUT_FOLDER_NAME = 'AJX_Phase1_Output'

# --- PROGRESS BAR ---
def print_main_bar(current_page, total_pages, status_msg="Processing"):
    percent = int((current_page / total_pages) * 100)
    length = 40
    filled_length = int(length * current_page // total_pages)
    bar = '█' * filled_length + '░' * (length - filled_length)
    sys.stdout.write(f'\r🏁 PROGRESS: |{bar}| {percent}% ({current_page}/{total_pages}) | {status_msg}')
    sys.stdout.flush()

# --- SETUP ---
keys_json = os.environ.get("GEMINI_API_KEYS_LIST")
# 👇 THIS IS THE NEW SECRET NAME
oauth_json = os.environ.get("GDRIVE_OAUTH_JSON")

if not keys_json or not oauth_json:
    log("\n❌ FATAL: Secrets missing. Script expects 'GDRIVE_OAUTH_JSON'.")
    exit()

try:
    API_KEYS = json.loads(keys_json)
    if not isinstance(API_KEYS, list): API_KEYS = [keys_json]
except:
    API_KEYS = [keys_json]

genai.configure(api_key=API_KEYS[0])
model = genai.GenerativeModel('gemini-1.5-flash')

# --- OAUTH CONNECTION (THE CRITICAL FIX) ---
try:
    # Load the User Token we generated on laptop
    token_info = json.loads(oauth_json)
    # 👇 This line connects as YOU, not the Robot
    creds = Credentials.from_authorized_user_info(token_info, ['https://www.googleapis.com/auth/drive'])
    service = build('drive', 'v3', credentials=creds)
    log("✅ Authenticated as USER (Using your 15GB Storage)")
except Exception as e:
    log(f"❌ OAuth Error: {e}")
    exit()

# --- HELPER FUNCTIONS ---

def get_folder_id(folder_name, parent_id=None):
    query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    if parent_id:
        query += f" and '{parent_id}' in parents"
    
    results = service.files().list(q=query, fields="files(id)").execute()
    files = results.get('files', [])
    return files[0]['id'] if files else None

def create_folder(folder_name, parent_id):
    existing = get_folder_id(folder_name, parent_id)
    if existing: return existing
    
    meta = {'name': folder_name, 'mimeType': 'application/vnd.google-apps.folder', 'parents': [parent_id]}
    folder = service.files().create(body=meta, fields='id').execute()
    return folder.get('id')

def download_latest_pdf(folder_id):
    query = f"'{folder_id}' in parents and mimeType = 'application/pdf' and trashed = false"
    results = service.files().list(q=query, orderBy='createdTime desc', pageSize=1).execute()
    items = results.get('files', [])
    if not items: return None
    
    file_id = items[0]['id']
    file_name = items[0]['name']
    log(f"⬇️  Found PDF: {file_name}")
    
    request = service.files().get_media(fileId=file_id)
    fh = io.FileIO(file_name, 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    log("✅ Download Complete.")
    return file_name

def get_existing_progress_map(book_folder_id):
    log("🔍 Scanning Drive for existing progress...")
    done_pages = set()
    query = f"'{book_folder_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    chapters = service.files().list(q=query, fields="files(id, name)").execute().get('files', [])
    for chap in chapters:
        img_query = f"'{chap['id']}' in parents and mimeType = 'image/jpeg' and trashed = false"
        images = service.files().list(q=img_query, fields="files(name)").execute().get('files', [])
        for img in images:
            try:
                name = img['name']
                num_part = name.split('_')[1].split('.')[0]
                done_pages.add(int(num_part))
            except:
                continue
    log(f"✅ Found {len(done_pages)} pages done.")
    return done_pages

def get_smart_chapters(pdf_path):
    log("\n🧠 AI INDEX ANALYSIS...")
    try:
        images = convert_from_path(pdf_path, first_page=1, last_page=20, dpi=100)
        prompt = """
        Analyze these images. Find the Table of Contents.
        Identify the STARTING PDF PAGE INDEX (0-based) for each chapter.
        Return strictly a JSON list: [{"name": "00_Preface", "start_index": 0}]
        """
        response = model.generate_content([prompt] + images)
        text = response.text.replace("```json", "").replace("```", "").strip()
        return json.loads(text)
    except:
        return []

def main():
    in_id = get_folder_id(INPUT_FOLDER_NAME)
    out_id = get_folder_id(OUTPUT_FOLDER_NAME)
    
    if not in_id or not out_id:
        log("❌ Input/Output Folders not found. (Check 'AJX_Input' exists in YOUR Drive)")
        return

    pdf_name = download_latest_pdf(in_id)
    if not pdf_name: 
        log("❌ No PDF found.")
        return

    book_name = pdf_name.replace('.pdf', '')
    book_folder_id = create_folder(book_name, out_id)
    
    try:
        info = pdfinfo_from_path(pdf_name)
        total_pages = int(info["Pages"])
    except:
        total_pages = 500
    
    done_pages_set = get_existing_progress_map(book_folder_id)
    
    chapters = get_smart_chapters(pdf_name)
    if not chapters: chapters = [{"name": "Full_Book", "start_index": 0}]
    
    log("📂 Pre-creating Folders...")
    chapter_drive_ids = []
    for i, chap in enumerate(chapters):
        safe_name = "".join(c for c in chap['name'] if c.isalnum() or c in (' ', '_')).strip()[:30]
        c_id = create_folder(f"{str(i+1).zfill(2)}_{safe_name}", book_folder_id)
        start = int(chap['start_index'])
        end = int(chapters[i+1]['start_index']) if i < len(chapters)-1 else total_pages
        chapter_drive_ids.append({'start': start, 'end': end, 'id': c_id})

    log("\n🚀 STARTING ENGINE... (OAuth User Mode)")
    
    chunk_size = 10
    for i in range(1, total_pages + 1, chunk_size):
        last_page = min(i + chunk_size - 1, total_pages)
        
        chunk_needed = False
        for p in range(i, last_page + 1):
            if p not in done_pages_set:
                chunk_needed = True
                break
        
        if not chunk_needed:
            print_main_bar(last_page, total_pages, "Skipping ⏭️")
            continue

        print_main_bar(i, total_pages, "Converting ⚙️")
        try:
            images = convert_from_path(pdf_name, first_page=i, last_page=last_page, dpi=150)
        except Exception as e:
            log(f"\n❌ Conversion Error: {e}")
            break

        for idx, img in enumerate(images):
            page_num = i + idx
            if page_num in done_pages_set: continue
                
            target_id = chapter_drive_ids[0]['id']
            for chap in chapter_drive_ids:
                if (page_num - 1) >= chap['start'] and (page_num - 1) < chap['end']:
                    target_id = chap['id']
                    break
            
            temp_name = f"page_{str(page_num).zfill(3)}.jpg"
            img.save(temp_name, "JPEG")
            
            file_meta = {'name': temp_name, 'parents': [target_id]}
            media = MediaFileUpload(temp_name, mimetype='image/jpeg')
            service.files().create(body=file_meta, media_body=media).execute()
            os.remove(temp_name)
            
        print_main_bar(last_page, total_pages, "Uploaded ✅")

    log("\n\n🎉 MISSION ACCOMPLISHED!")

if __name__ == "__main__":
    main()
