print("╔════════════════════════════════════════════════════╗")
print("║   AJX ULTIMATE: SELF-HEALING & VERIFY              ║")
print("║   (Runs once. Skips finished tasks. Fills gaps.)   ║")
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
USER_INPUT_STR = os.environ.get("USER_PROVIDED_INPUT", "").strip()

def log(msg):
    print(msg)
    sys.stdout.flush()

def clean_page_num(value):
    if isinstance(value, int): return value
    try:
        cleaned = str(value).replace("l", "1").replace("O", "0").replace("o", "0")
        digits = re.findall(r'\d+', cleaned)
        if digits: return int(digits[0])
    except:
        pass
    return 1

def clean_filename(name):
    return "".join(c for c in name if c.isalnum() or c in (' ', '_', '-')).strip()[:40]

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

def count_files_in_folder(folder_id):
    """Checks how many images are currently inside a folder."""
    query = f"'{folder_id}' in parents and mimeType = 'image/jpeg' and trashed = false"
    # We use pageSize=1000 to be safe, though most chapters are smaller
    results = service.files().list(q=query, pageSize=1000, fields="files(id)").execute()
    return len(results.get('files', []))

def download_latest_pdf(folder_id):
    query = f"'{folder_id}' in parents and mimeType = 'application/pdf' and trashed = false"
    results = service.files().list(q=query, orderBy='createdTime desc', pageSize=1).execute()
    items = results.get('files', [])
    if not items: return None, None
    return items[0]['name'], items[0]['id']

def perform_download(file_id, file_name):
    log(f"⬇️ Downloading PDF: {file_name}...")
    request = service.files().get_media(fileId=file_id)
    fh = io.FileIO(file_name, 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done: status, done = downloader.next_chunk()
    fh.close() 

def check_json_exists(folder_id, json_name):
    query = f"name = '{json_name}' and '{folder_id}' in parents and trashed = false"
    results = service.files().list(q=query, fields="files(id)").execute()
    return len(results.get('files', [])) > 0

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

def cleanup_previews(folder_id):
    query = f"'{folder_id}' in parents and (name contains 'COLLAGE' or name contains 'PREVIEW') and trashed = false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get('files', [])
    if files:
        for f in files:
            try: service.files().delete(fileId=f['id']).execute()
            except: pass

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

def find_and_preview_index(pdf_name, book_folder_id):
    log("\n🕵️ MODE 1: SCOUTING FOR INDEX...")
    images = convert_from_path(pdf_name, first_page=1, last_page=50, dpi=100)
    prompt = """Identify TOP 4 pages that look like the Table of Contents.
    Output JSON: {"candidate_pages": [5, 6]}"""
    try:
        response = model.generate_content([prompt] + images[:30])
        text = response.text.replace("```json", "").replace("```", "").strip()
        candidate_pages = json.loads(text).get('candidate_pages', [1,2,3,4])
    except: candidate_pages = [1, 2, 3, 4]

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
        log("\n" + "="*60)
        log(f"🔗 CLICK LINK: {file.get('webViewLink')}")
        log("="*60 + "\n")

# --- 🔄 RECURSIVE MATH & FOLDER CREATION ---
def calculate_ranges_recursive(node_list, end_limit):
    node_list.sort(key=lambda x: clean_page_num(x.get('start_page', 1)))
    for i, node in enumerate(node_list):
        start_p = clean_page_num(node.get('start_page', 1))
        if i < len(node_list) - 1:
            next_p = clean_page_num(node_list[i+1].get('start_page', start_p))
            end_p = max(start_p, next_p - 1)
        else:
            end_p = end_limit 
        node['start_page'] = start_p
        node['end_page'] = end_p
        
        if node.get('subtopics'):
            calculate_ranges_recursive(node['subtopics'], end_p)

def create_folders_recursive(node_list, parent_folder_id, map_list):
    for i, node in enumerate(node_list):
        # Create Folder
        folder_name = f"{str(i+1).zfill(2)}_{clean_filename(node['chapter_name'])}"
        fid = create_folder(folder_name, parent_folder_id)
        
        # Check Children
        if node.get('subtopics'):
            create_folders_recursive(node['subtopics'], fid, map_list)
        else:
            # Leaf Node -> Add to Map for Images
            map_list.append({
                'start': int(node['start_page']),
                'end': int(node['end_page']),
                'id': fid,
                'name': node['chapter_name']
            })

def generate_index_from_range(pdf_name, input_str, total_pages, book_folder_id, json_name):
    cleanup_previews(book_folder_id)
    log(f"\n🏗️ MODE 2: BUILDING INDEX FROM RANGE '{input_str}'...")
    
    if "-" in input_str:
        start, end = map(int, input_str.split("-"))
        pages_to_read = list(range(start, end + 1))
    else:
        pages_to_read = [int(input_str.strip())]
    
    chunk_start, chunk_end = min(pages_to_read), max(pages_to_read)
    images = convert_from_path(pdf_name, first_page=chunk_start, last_page=chunk_end, dpi=300)
    
    prompt = """
    Analyze Table of Contents. Detect Hierarchy (Units -> Chapters -> Topics).
    Output JSON (Recursive):
    [{"chapter_name": "Unit I", "start_page": 1, "subtopics": [{"chapter_name": "Topic A", "start_page": 1, "subtopics": []}]}]
    """
    
    try:
        response = model.generate_content([prompt] + images)
        text = response.text.replace("```json", "").replace("```", "").strip()
        toc_data = json.loads(text)
    except:
        toc_data = [{"chapter_name": "Full_Book", "start_page": 1, "subtopics": []}]
        
    log("Cc Calculating Ranges...")
    calculate_ranges_recursive(toc_data, total_pages)

    with open(json_name, 'w', encoding='utf-8') as f:
        json.dump(toc_data, f, indent=4, ensure_ascii=False)
    
    file_meta = {'name': json_name, 'parents': [book_folder_id]}
    media = MediaFileUpload(json_name, mimetype='application/json')
    service.files().create(body=file_meta, media_body=media).execute()
    log(f"✅ JSON Created: {json_name}")
    return toc_data

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

    toc_data = []
    
    # --- TASK 1: JSON CHECK ---
    if check_json_exists(book_folder_id, json_name):
        log("✅ Task 1: JSON Index exists. Skipping AI.")
        toc_data = download_json(book_folder_id, json_name)
    else:
        log("⚠️ Task 1: JSON missing. Starting generation...")
        perform_download(pdf_id, pdf_name)
        if not USER_INPUT_STR:
            find_and_preview_index(pdf_name, book_folder_id)
            log("\n🛑 STOPPING. Check Link.")
            return
        else:
            try: info = pdfinfo_from_path(pdf_name); total_pages = int(info["Pages"])
            except: total_pages = 500
            toc_data = generate_index_from_range(pdf_name, USER_INPUT_STR, total_pages, book_folder_id, json_name)

    # --- TASK 2: FOLDER VERIFICATION ---
    log("\n📂 Task 2: Verifying Folder Structure...")
    chapter_map = [] 
    create_folders_recursive(toc_data, book_folder_id, chapter_map)
    log(f"✅ Task 2 Complete: Mapped {len(chapter_map)} folders.")

    # --- TASK 3: IMAGE VERIFICATION & FILLING ---
    log("\n🚀 Task 3: Verifying Images in Folders...")
    if not os.path.exists(pdf_name): perform_download(pdf_id, pdf_name)
    
    # Loop through CHAPTERS, not pages. This is the "Self-Healing" logic.
    all_complete = True
    
    for i, chap in enumerate(chapter_map):
        expected_count = chap['end'] - chap['start'] + 1
        current_count = count_files_in_folder(chap['id'])
        
        log(f"   -> Checking [{chap['name']}] (Pages {chap['start']}-{chap['end']})")
        
        if current_count >= expected_count:
            # FOLDER IS FULL -> SKIP
            pass 
        else:
            # FOLDER IS EMPTY OR PARTIAL -> REFILL
            all_complete = False
            log(f"      ⚠️ Missing Images! (Found {current_count}, Need {expected_count}) -> Converting...")
            
            try:
                # Targeted Conversion: Convert ONLY this chapter's range
                images = convert_from_path(pdf_name, first_page=chap['start'], last_page=chap['end'], dpi=150)
                
                for idx, img in enumerate(images):
                    # Natural Numbering
                    file_num = idx + 1
                    file_name = f"{file_num}.jpg"
                    
                    # Save locally
                    img.save(file_name, "JPEG")
                    
                    # Upload
                    file_meta = {'name': file_name, 'parents': [chap['id']]}
                    media = MediaFileUpload(file_name, mimetype='image/jpeg')
                    service.files().create(body=file_meta, media_body=media).execute()
                    os.remove(file_name)
                    
                log(f"      🎉 Refilled {len(images)} images.")
                
            except Exception as e:
                log(f"      ❌ Error converting chapter: {e}")

    if all_complete:
        log("\n🎉 ALL TASKS COMPLETE! No missing files found.")
    else:
        log("\n✅ CYCLE COMPLETE. Run again if any timeouts occurred.")

if __name__ == "__main__":
    main()
