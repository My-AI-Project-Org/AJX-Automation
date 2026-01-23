print("╔════════════════════════════════════════════════════╗")
print("║   AJX ULTIMATE: DEBUG MODE (SHOWS REAL ERRORS)     ║")
print("╚════════════════════════════════════════════════════╝")

import os
import io
import json
import time
import shutil
import sys
import traceback  # Added for detailed errors

# Google Libraries
from google.oauth2.credentials import Credentials 
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
import google.generativeai as genai
from pdf2image import convert_from_path, pdfinfo_from_path

# --- CONFIGURATION ---
INPUT_FOLDER_NAME = 'AJX_Input'
OUTPUT_FOLDER_NAME = 'AJX_Phase1_Output'

def log(msg):
    print(msg)
    sys.stdout.flush()

def print_bar(current, total, msg="Processing"):
    percent = int((current / total) * 100)
    bar = '█' * int(20 * current // total) + '░' * (20 - int(20 * current // total))
    sys.stdout.write(f'\r🏁 {msg}: |{bar}| {percent}% ({current}/{total})')
    sys.stdout.flush()

# --- AUTHENTICATION ---
keys_json = os.environ.get("GEMINI_API_KEYS_LIST")
oauth_json = os.environ.get("GDRIVE_OAUTH_JSON")

if not keys_json or not oauth_json:
    log("\n❌ FATAL: Secrets missing.")
    exit()

try:
    API_KEYS = json.loads(keys_json)
    if not isinstance(API_KEYS, list): API_KEYS = [keys_json]
    genai.configure(api_key=API_KEYS[0])
    model = genai.GenerativeModel('gemini-1.5-flash')
except Exception as e:
    log(f"❌ Gemini Setup Error: {e}")
    exit()

try:
    token_info = json.loads(oauth_json)
    creds = Credentials.from_authorized_user_info(token_info, ['https://www.googleapis.com/auth/drive'])
    service = build('drive', 'v3', credentials=creds)
    log("✅ Authenticated as USER.")
except Exception as e:
    log(f"❌ Drive Auth Error: {e}")
    exit()

# --- CORE FUNCTIONS ---

def get_folder_id(folder_name, parent_id=None):
    query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    if parent_id: query += f" and '{parent_id}' in parents"
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
    if not items: return None, None
    
    file_id = items[0]['id']
    file_name = items[0]['name']
    
    request = service.files().get_media(fileId=file_id)
    fh = io.FileIO(file_name, 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    return file_name, file_id

def check_json_exists(folder_id, json_name):
    query = f"name = '{json_name}' and '{folder_id}' in parents and trashed = false"
    results = service.files().list(q=query, fields="files(id)").execute()
    return len(results.get('files', [])) > 0

def check_images_exist(folder_id):
    query = f"'{folder_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    chapters = service.files().list(q=query, fields="files(id)").execute().get('files', [])
    if not chapters: return False
    c_id = chapters[0]['id']
    img_query = f"'{c_id}' in parents and mimeType = 'image/jpeg' and trashed = false"
    imgs = service.files().list(q=img_query, pageSize=1, fields="files(id)").execute().get('files', [])
    return len(imgs) > 0

def analyze_and_upload_json(pdf_name, total_pages, book_folder_id, json_name):
    log("\n🧠 STARTING AI ANALYSIS (Generating Index)...")
    
    try:
        # 1. TEST CONVERSION
        log("   -> Converting first 20 pages to images...")
        images = convert_from_path(pdf_name, first_page=1, last_page=20, dpi=100)
        log("   -> Conversion success. Sending to Gemini...")
        
        prompt = """
        Analyze these images. Find the Table of Contents.
        Create a DETAILED JSON map of the book with Chapters and Subtopics.
        OUTPUT FORMAT (Strict JSON):
        [{"chapter_name": "Unit 1", "start_page": 5, "subtopics": []}]
        RULES: Return ONLY valid JSON.
        """
        
        response = model.generate_content([prompt] + images)
        log("   -> Gemini responded. Parsing JSON...")
        
        clean_text = response.text.replace("```json", "").replace("```", "").strip()
        toc_data = json.loads(clean_text)
        
    except Exception as e:
        # 👇 THIS IS THE NEW PART THAT WILL SHOW US THE ERROR
        log(f"\n❌ AI ANALYSIS CRASHED: {e}")
        log(f"❌ DETAILED TRACEBACK: {traceback.format_exc()}")
        if 'response' in locals() and hasattr(response, 'text'):
            log(f"❌ RAW MODEL RESPONSE: {response.text}")
        
        log("⚠️ Using Default Index (Full Book) due to error.")
        toc_data = [{"chapter_name": "Full_Book", "start_page": 1, "subtopics": []}]
        
    # Math Logic
    log("Cc Calculating Page Ranges...")
    for i, chap in enumerate(toc_data):
        start = int(chap.get('start_page', 1))
        if i < len(toc_data) - 1:
            next_start = int(toc_data[i+1].get('start_page', start))
            end = max(start, next_start - 1)
        else:
            end = total_pages
        chap['start_page'] = start
        chap['end_page'] = end

    # Upload
    with open(json_name, 'w', encoding='utf-8') as f:
        json.dump(toc_data, f, indent=4, ensure_ascii=False)
    
    file_meta = {'name': json_name, 'parents': [book_folder_id]}
    media = MediaFileUpload(json_name, mimetype='application/json')
    service.files().create(body=file_meta, media_body=media).execute()
    log(f"✅ Uploaded Index: {json_name}")
    os.remove(json_name)
    return toc_data

# --- MAIN LOGIC ---

def main():
    in_id = get_folder_id(INPUT_FOLDER_NAME)
    out_id = get_folder_id(OUTPUT_FOLDER_NAME)
    
    if not in_id or not out_id:
        log("❌ Input/Output Folders not found.")
        return

    log("🔍 Looking for PDF...")
    pdf_name, pdf_id = download_latest_pdf(in_id)
    if not pdf_name: 
        log("❌ No PDF found in AJX_Input.")
        return

    book_name = pdf_name.replace('.pdf', '')
    json_name = f"{book_name}_index.json"
    
    log(f"📘 Processing: {book_name}")
    
    book_folder_id = create_folder(book_name, out_id)
    
    # FORCE RE-RUN AI: We assume if the user is running this, they want to fix the Index.
    # So we ignore 'json_exists' check for this debug run.
    toc_data = analyze_and_upload_json(pdf_name, 500, book_folder_id, json_name)

    log("\n🎉 DEBUG RUN COMPLETE. CHECK LOGS ABOVE FOR RED ERRORS!")

if __name__ == "__main__":
    main()
