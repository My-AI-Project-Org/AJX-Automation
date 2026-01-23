print("╔════════════════════════════════════════════════════╗")
print("║   AJX ULTIMATE: ROBUST MATH & ERROR HANDLING       ║")
print("╚════════════════════════════════════════════════════╝")

import os
import io
import json
import time
import shutil
import sys
import re  # Import Regex for cleaning numbers

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

# --- HELPER: CLEAN PAGE NUMBERS (THE FIX) ---
def clean_page_num(value, default=1):
    """
    Forces 'B9', 'Page 10', 'iv' into a simple integer.
    """
    if isinstance(value, int): return value
    
    # 1. Try simple conversion
    try:
        return int(str(value).strip())
    except:
        pass
    
    # 2. Extract digits only (e.g., "B9" -> 9)
    digits = re.findall(r'\d+', str(value))
    if digits:
        return int(digits[0])
    
    # 3. If totally failed, return default
    return default

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
        # Convert first 20 pages for AI
        images = convert_from_path(pdf_name, first_page=1, last_page=20, dpi=100)
        
        prompt = """
        Analyze these images. Find the Table of Contents.
        Create a DETAILED JSON map of the book.
        OUTPUT FORMAT (Strict JSON):
        [{"chapter_name": "Unit 1", "start_page": 5, "subtopics": []}]
        RULES: 
        1. If page is 'B9', just return 9.
        2. Return ONLY valid JSON.
        """
        
        response = model.generate_content([prompt] + images)
        clean_text = response.text.replace("```json", "").replace("```", "").strip()
        toc_data = json.loads(clean_text)
        
    except Exception as e:
        log(f"⚠️ AI Analysis failed ({e}). Using Default Index.")
        toc_data = [{"chapter_name": "Full_Book", "start_page": 1, "subtopics": []}]
        
    # --- MATH LOGIC WITH CLEANING ---
    log("Cc Calculating Page Ranges (Safe Mode)...")
    
    # Sort just in case AI returned them out of order
    # Use clean_page_num to avoid sorting errors
    try:
        toc_data.sort(key=lambda x: clean_page_num(x.get('start_page', 0)))
    except:
        pass # If sorting fails, just use original order

    for i, chap in enumerate(toc_data):
        # 👇 HERE IS THE FIX: We clean the number before using it
        start = clean_page_num(chap.get('start_page', 1))
        
        if i < len(toc_data) - 1:
            next_start = clean_page_num(toc_data[i+1].get('start_page', start))
            end = max(start, next_start - 1)
        else:
            end = total_pages
        
        chap['start_page'] = start
        chap['end_page'] = end

        # Clean subtopics too
        subs = chap.get('subtopics', [])
        for j, sub in enumerate(subs):
            s_start = clean_page_num(sub.get('start_page', start))
            if j < len(subs) - 1:
                s_next = clean_page_num(subs[j+1].get('start_page', s_start))
                s_end = max(s_start, s_next - 1)
            else:
                s_end = end
            sub['start_page'] = s_start
            sub['end_page'] = s_end
            sub['range'] = f"{s_start}-{s_end}"

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
    
    # CHECK STATUS
    json_exists = check_json_exists(book_folder_id, json_name)
    
    # We will FORCE generate JSON if it failed last time (meaning it exists but might be corrupt or incomplete)
    # But for safety, let's just run logic:
    
    try:
        info = pdfinfo_from_path(pdf_name)
        total_pages = int(info["Pages"])
    except:
        total_pages = 500

    if json_exists:
        log("✅ Index JSON already exists.")
        # Optional: Download it to check chapters? For now we skip.
        toc_data = [{"chapter_name": "Full_Book", "start_page": 1, "end_page": total_pages}]
    else:
        toc_data = analyze_and_upload_json(pdf_name, total_pages, book_folder_id, json_name)

    # CHECK IMAGES
    if check_images_exist(book_folder_id):
        log("✅ Images already exist. Job Done.")
        return

    log("\n🚀 STARTING IMAGE CONVERSION...")
    
    # Create Folders
    chapter_drive_ids = []
    # If toc_data is just the dummy, we try to load the real one if possible, else default
    # (Simplified for stability)
    for i, chap in enumerate(toc_data):
        safe_name = "".join(c for c in chap['chapter_name'] if c.isalnum() or c in (' ', '_')).strip()[:30]
        c_id = create_folder(f"{str(i+1).zfill(2)}_{safe_name}", book_folder_id)
        chapter_drive_ids.append({'start': int(chap['start_page']), 'end': int(chap['end_page']), 'id': c_id})

    # Conversion Loop
    chunk_size = 10
    for i in range(1, total_pages + 1, chunk_size):
        last_page = min(i + chunk_size - 1, total_pages)
        print_bar(i, total_pages, "Converting")
        
        try:
            images = convert_from_path(pdf_name, first_page=i, last_page=last_page, dpi=150)
        except:
            continue

        for idx, img in enumerate(images):
            page_num = i + idx
            
            target_id = chapter_drive_ids[0]['id']
            for chap in chapter_drive_ids:
                if (page_num) >= chap['start'] and (page_num) <= chap['end']:
                    target_id = chap['id']
                    break
            
            temp_name = f"page_{str(page_num).zfill(3)}.jpg"
            img.save(temp_name, "JPEG")
            
            file_meta = {'name': temp_name, 'parents': [target_id]}
            media = MediaFileUpload(temp_name, mimetype='image/jpeg')
            service.files().create(body=file_meta, media_body=media).execute()
            os.remove(temp_name)

    log("\n\n🎉 FULL SUCCESS!")

if __name__ == "__main__":
    main()
