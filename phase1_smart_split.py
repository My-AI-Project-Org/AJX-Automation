print("╔════════════════════════════════════════════════════╗")
print("║   AJX ULTIMATE: AUTO-CLEANUP EDITION               ║")
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
from PIL import Image, ImageDraw, ImageFont

# --- CONFIGURATION ---
INPUT_FOLDER_NAME = 'AJX_Input'
OUTPUT_FOLDER_NAME = 'AJX_Phase1_Output'
# "25" or "25-27"
USER_INPUT_STR = os.environ.get("USER_PROVIDED_INPUT", "").strip()

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
    model = genai.GenerativeModel('gemini-2.5-flash')
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
    file_id = items[0]['id']
    file_name = items[0]['name']
    
    log(f"⬇️ Downloading PDF: {file_name}...")
    request = service.files().get_media(fileId=file_id)
    fh = io.FileIO(file_name, 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done: status, done = downloader.next_chunk()
    fh.close() 
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

# --- CLEANUP FUNCTION (NEW) ---
def cleanup_previews(folder_id):
    # Finds any file with 'COLLAGE' or 'PREVIEW' in name and deletes it
    query = f"'{folder_id}' in parents and (name contains 'COLLAGE' or name contains 'PREVIEW') and trashed = false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get('files', [])
    
    if files:
        log(f"🧹 Cleaning up {len(files)} temporary preview files...")
        for f in files:
            try:
                service.files().delete(fileId=f['id']).execute()
                log(f"   -> Deleted: {f['name']}")
            except:
                pass
    else:
        log("✨ No temp files to clean.")

# --- COLLAGE CREATOR ---
def create_collage(image_list, labels, output_name):
    thumbnails = []
    for img in image_list:
        thumb = img.copy()
        thumb.thumbnail((600, 800))
        thumbnails.append(thumb)
    
    w, h = thumbnails[0].size
    grid_img = Image.new('RGB', (w*2 + 20, h*2 + 20), (255, 255, 255))
    draw = ImageDraw.Draw(grid_img)
    positions = [(0,0), (w+10, 0), (0, h+10), (w+10, h+10)]
    
    for i, thumb in enumerate(thumbnails):
        if i >= 4: break
        pos = positions[i]
        grid_img.paste(thumb, pos)
        draw.rectangle([pos[0], pos[1], pos[0]+120, pos[1]+50], fill="red")
        draw.text((pos[0]+10, pos[1]+10), f"Page {labels[i]}", fill="white")
        
    grid_img.save(output_name)
    return output_name

# --- MODE 1: SCOUT ---
def find_and_preview_index(pdf_name, book_folder_id):
    log("\n🕵️ MODE 1: SCOUTING FOR INDEX (TOP 4 CANDIDATES)...")
    images = convert_from_path(pdf_name, first_page=1, last_page=50, dpi=100)
    
    prompt = """
    Identify TOP 4 pages that look like the Table of Contents (Index).
    Output JSON: {"candidate_pages": [5, 22, 25, 26]}
    """
    try:
        response = model.generate_content([prompt] + images[:30])
        text = response.text.replace("```json", "").replace("```", "").strip()
        data = json.loads(text)
        candidate_pages = data.get('candidate_pages', [])
    except:
        candidate_pages = [1, 2, 3, 4]

    if not candidate_pages: candidate_pages = [1, 2, 3, 4]
    candidate_pages = list(dict.fromkeys(candidate_pages))[:4]
    
    collage_images = []
    valid_labels = []
    for p in candidate_pages:
        idx = int(p) - 1
        if idx < len(images):
            collage_images.append(images[idx])
            valid_labels.append(p)
    
    if collage_images:
        preview_name = "INDEX_CANDIDATES_COLLAGE.jpg"
        create_collage(collage_images, valid_labels, preview_name)
        
        file_meta = {'name': preview_name, 'parents': [book_folder_id]}
        media = MediaFileUpload(preview_name, mimetype='image/jpeg')
        file = service.files().create(body=file_meta, media_body=media, fields='id, webViewLink').execute()
        
        link = file.get('webViewLink')
        
        log("\n" + "="*60)
        log(f"🔗 CLICK THIS LINK TO SEE THE CANDIDATES:")
        log(f"👉 {link}")
        log("="*60 + "\n")
        log("NOTE: This image will be AUTO-DELETED when you run Step 2.")
    else:
        log("❌ Error creating collage.")

# --- MODE 2: MULTI-PAGE BUILDER ---
def generate_index_from_range(pdf_name, input_str, total_pages, book_folder_id, json_name):
    # 1. CLEANUP FIRST!
    cleanup_previews(book_folder_id)

    log(f"\n🏗️ MODE 2: BUILDING INDEX FROM RANGE '{input_str}'...")
    
    pages_to_read = []
    if "-" in input_str:
        start_s, end_s = input_str.split("-")
        start = int(start_s.strip())
        end = int(end_s.strip())
        pages_to_read = list(range(start, end + 1))
    else:
        pages_to_read = [int(input_str.strip())]
    
    chunk_start = min(pages_to_read)
    chunk_end = max(pages_to_read)
    
    raw_images = convert_from_path(pdf_name, first_page=chunk_start, last_page=chunk_end, dpi=200)
    images = raw_images 
    
    prompt = """
    Extract all chapters from ALL images and merge them into a SINGLE JSON list.
    Output JSON: 
    [{"chapter_name": "History", "start_page": 5, "subtopics": []}]
    Rules: 
    1. 'start_page' must be the actual page number in the book.
    2. Clean page numbers.
    """
    
    try:
        response = model.generate_content([prompt] + images)
        text = response.text.replace("```json", "").replace("```", "").strip()
        toc_data = json.loads(text)
    except:
        log("⚠️ AI Failed to read index. Using Default.")
        toc_data = [{"chapter_name": "Full_Book", "start_page": 1, "subtopics": []}]
        
    log("Cc Calculating Ranges & Sorting...")
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

    # --- STEP 1: PRIORITIZE JSON CREATION ---
    toc_data = []
    
    if check_json_exists(book_folder_id, json_name):
        log("✅ Index JSON found in Drive.")
        toc_data = download_json(book_folder_id, json_name)
    else:
        if not USER_INPUT_STR:
            find_and_preview_index(pdf_name, book_folder_id)
            log("\n🛑 STOPPING. Check the Collage Link above.")
            return
        else:
            try:
                info = pdfinfo_from_path(pdf_name)
                total_pages = int(info["Pages"])
            except:
                total_pages = 500
            
            # Step 2: Build (This will run cleanup_previews first)
            toc_data = generate_index_from_range(pdf_name, USER_INPUT_STR, total_pages, book_folder_id, json_name)

    # --- STEP 2: CHECK IMAGES ---
    if check_images_exist(book_folder_id):
        log("✅ Images already exist in Drive. JSON is safe. Job Done.")
        return

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
