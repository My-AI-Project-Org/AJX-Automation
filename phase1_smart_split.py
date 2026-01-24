print("╔════════════════════════════════════════════════════╗")
print("║   AJX ULTIMATE: MAGIC LINK VERIFICATION            ║")
print("╚════════════════════════════════════════════════════╝")

import os
import io
import json
import time
import shutil
import sys
import re

# Google Libraries
from google.oauth2.credentials import Credentials 
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
import google.generativeai as genai
from pdf2image import convert_from_path, pdfinfo_from_path

# --- CONFIGURATION ---
INPUT_FOLDER_NAME = 'AJX_Input'
OUTPUT_FOLDER_NAME = 'AJX_Phase1_Output'
USER_CONFIRMED_PAGE = os.environ.get("USER_PROVIDED_PAGE", "").strip()

def log(msg):
    print(msg)
    sys.stdout.flush()

def print_bar(current, total, msg="Processing"):
    try:
        percent = int((current / total) * 100)
        bar = '█' * int(20 * current // total) + '░' * (20 - int(20 * current // total))
        sys.stdout.write(f'\r🏁 {msg}: |{bar}| {percent}% ({current}/{total})')
        sys.stdout.flush()
    except:
        pass

def clean_page_num(value, default=1):
    if isinstance(value, int): return value
    try:
        hindi_digits = str(value).maketrans("०१२३४५६७८९", "0123456789")
        cleaned = str(value).translate(hindi_digits)
        digits = re.findall(r'\d+', cleaned)
        if digits: return int(digits[0])
    except:
        pass
    return default

# --- AUTH ---
keys_json = os.environ.get("GEMINI_API_KEYS_LIST")
oauth_json = os.environ.get("GDRIVE_OAUTH_JSON")

if not keys_json or not oauth_json:
    log("❌ FATAL: Secrets missing.")
    exit()

try:
    API_KEYS = json.loads(keys_json)
    if not isinstance(API_KEYS, list): API_KEYS = [keys_json]
    genai.configure(api_key=API_KEYS[0])
    model = genai.GenerativeModel('gemini-1.5-flash')
except:
    log("❌ Gemini Error")
    exit()

try:
    token_info = json.loads(oauth_json)
    creds = Credentials.from_authorized_user_info(token_info, ['https://www.googleapis.com/auth/drive'])
    service = build('drive', 'v3', credentials=creds)
    log("✅ Authenticated as USER.")
except:
    log("❌ Drive Auth Error")
    exit()

# --- DRIVE UTILS ---
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
    return items[0]['name'], items[0]['id']

# --- CHECKS ---
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

def download_json(folder_id, json_name):
    query = f"name = '{json_name}' and '{folder_id}' in parents and trashed = false"
    results = service.files().list(q=query, fields="files(id)").execute()
    if not results.get('files'): return None
    file_id = results['files'][0]['id']
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done: status, done = downloader.next_chunk()
    fh.seek(0)
    return json.load(fh)

# --- MODE 1: FIND & GENERATE LINK ---
def find_and_preview_index(pdf_name, book_folder_id):
    log("\n🕵️ MODE 1: SCOUTING FOR INDEX PAGE...")
    log("   -> Scanning first 50 pages...")
    images = convert_from_path(pdf_name, first_page=1, last_page=50, dpi=150)
    
    prompt = """
    Look at these images. Identify the 'Table of Contents' (Index/Vishay Suchi).
    Return the EXACT Page Number where it starts.
    Output JSON: {"found_page": 25}
    If not found, return {"found_page": null}
    """
    
    found_page = 1
    try:
        response = model.generate_content([prompt] + images[:30])
        text = response.text.replace("```json", "").replace("```", "").strip()
        data = json.loads(text)
        found_page = data.get('found_page')
        if not found_page: raise Exception("Next batch")
    except:
        try:
            response = model.generate_content([prompt] + images[30:])
            text = response.text.replace("```json", "").replace("```", "").strip()
            data = json.loads(text)
            found_page = data.get('found_page')
        except:
            found_page = 1
            
    if not found_page: found_page = 1
    found_page = int(found_page)

    log(f"📸 AI thinks Index is on Page: {found_page}")
    
    img_index = found_page - 1
    if img_index < len(images):
        preview_name = f"PREVIEW_INDEX_PAGE_{found_page}.jpg"
        images[img_index].save(preview_name)
        
        file_meta = {'name': preview_name, 'parents': [book_folder_id]}
        media = MediaFileUpload(preview_name, mimetype='image/jpeg')
        # 👇 KEY CHANGE: Request webViewLink
        file = service.files().create(body=file_meta, media_body=media, fields='id, webViewLink').execute()
        
        link = file.get('webViewLink')
        
        log("\n" + "="*60)
        log(f"🔗 CLICK THIS LINK TO SEE THE IMAGE:")
        log(f"👉 {link}")
        log("="*60 + "\n")
        
        log("1. Click the link above.")
        log(f"2. Verify if it is the Index. (It says Page {found_page})")
        log("3. If correct, RUN THIS WORKFLOW AGAIN and type that number!")
    else:
        log("❌ Could not extract preview (Page out of range).")

# --- MODE 2: BUILD JSON ---
def generate_index_from_page(pdf_name, page_num, total_pages, book_folder_id, json_name):
    log(f"\n🏗️ MODE 2: BUILDING INDEX FROM PAGE {page_num}...")
    start = int(page_num)
    end = min(start + 4, total_pages)
    images = convert_from_path(pdf_name, first_page=start, last_page=end, dpi=200)
    
    prompt = """
    Extract the Table of Contents from these images.
    Output JSON: 
    [{"chapter_name": "History", "start_page": 5, "subtopics": []}]
    Rules: 
    1. 'start_page' must be the actual page number in the book.
    2. Clean page numbers (remove 'A', 'B').
    """
    try:
        response = model.generate_content([prompt] + images)
        text = response.text.replace("```json", "").replace("```", "").strip()
        toc_data = json.loads(text)
    except:
        log("⚠️ AI Failed to read index. Using Default.")
        toc_data = [{"chapter_name": "Full_Book", "start_page": 1, "subtopics": []}]
        
    log("Cc Calculating Ranges...")
    toc_data.sort(key=lambda x: clean_page_num(x.get('start_page', 1)))
    for i, chap in enumerate(toc_data):
        start_p = clean_page_num(chap.get('start_page', 1))
        if i < len(toc_data) - 1:
            next_p = clean_page_num(toc_data[i+1].get('start_page', start_p))
            end_p = max(start_p, next_p - 1)
        else:
            end_p = total_pages
        chap['start_page'] = start_p
        chap['end_page'] = end_p
        
        for sub in chap.get('subtopics', []):
            s_start = clean_page_num(sub.get('start_page', start_p))
            sub['start_page'] = s_start
            sub['end_page'] = end_p
            sub['range'] = f"{s_start}-{end_p}"

    with open(json_name, 'w', encoding='utf-8') as f:
        json.dump(toc_data, f, indent=4, ensure_ascii=False)
    
    file_meta = {'name': json_name, 'parents': [book_folder_id]}
    media = MediaFileUpload(json_name, mimetype='application/json')
    service.files().create(body=file_meta, media_body=media).execute()
    log(f"✅ JSON Created: {json_name}")
    return toc_data

# --- MAIN ---
def main():
    in_id = get_folder_id(INPUT_FOLDER_NAME)
    out_id = get_folder_id(OUTPUT_FOLDER_NAME)
    if not in_id or not out_id: return

    log("🔍 Looking for PDF...")
    pdf_name, pdf_id = download_latest_pdf(in_id)
    if not pdf_name: return

    book_name = pdf_name.replace('.pdf', '')
    json_name = f"{book_name}_index.json"
    book_folder_id = create_folder(book_name, out_id)

    # --- CHECK 1: IMAGES EXIST? ---
    if check_images_exist(book_folder_id):
        log("✅ Images already exist in Drive. Skipping all work.")
        return

    # --- CHECK 2: JSON EXISTS? ---
    toc_data = []
    if check_json_exists(book_folder_id, json_name):
        log("✅ Index JSON found in Drive. Skipping Verification.")
        toc_data = download_json(book_folder_id, json_name)
    else:
        # NO JSON -> CHECK USER INPUT
        if not USER_CONFIRMED_PAGE:
            # STEP 1: SCOUT & PREVIEW
            find_and_preview_index(pdf_name, book_folder_id)
            log("\n🛑 STOPPING. Please Click the Link above, then run again with the Page Number.")
            return
        else:
            # STEP 2: BUILD
            log(f"✅ USER INPUT: Index is on Page {USER_CONFIRMED_PAGE}")
            try:
                info = pdfinfo_from_path(pdf_name)
                total_pages = int(info["Pages"])
            except:
                total_pages = 500
            toc_data = generate_index_from_page(pdf_name, USER_CONFIRMED_PAGE, total_pages, book_folder_id, json_name)

    # --- STEP 3: CONVERT IMAGES ---
    if not toc_data: return

    log("\n🚀 STARTING IMAGE CONVERSION...")
    try:
        info = pdfinfo_from_path(pdf_name)
        total_pages = int(info["Pages"])
    except:
        total_pages = 500

    chapter_ids = []
    for i, chap in enumerate(toc_data):
        safe_name = "".join(c for c in chap['chapter_name'] if c.isalnum() or c in (' ', '_')).strip()[:30]
        c_id = create_folder(f"{str(i+1).zfill(2)}_{safe_name}", book_folder_id)
        chapter_ids.append({'start': int(chap['start_page']), 'end': int(chap['end_page']), 'id': c_id})

    chunk_size = 10
    for i in range(1, total_pages + 1, chunk_size):
        last_page = min(i + chunk_size - 1, total_pages)
        print_bar(i, total_pages, "Converting")
        try:
            images = convert_from_path(pdf_name, first_page=i, last_page=last_page, dpi=150)
        except: continue

        for idx, img in enumerate(images):
            page_num = i + idx
            target_id = chapter_ids[0]['id']
            for chap in chapter_ids:
                if page_num >= chap['start'] and page_num <= chap['end']:
                    target_id = chap['id']
                    break
            
            temp_name = f"page_{str(page_num).zfill(3)}.jpg"
            img.save(temp_name, "JPEG")
            file_meta = {'name': temp_name, 'parents': [target_id]}
            media = MediaFileUpload(temp_name, mimetype='image/jpeg')
            service.files().create(body=file_meta, media_body=media).execute()
            os.remove(temp_name)

    log("\n🎉 FULL SUCCESS!")

if __name__ == "__main__":
    main()
