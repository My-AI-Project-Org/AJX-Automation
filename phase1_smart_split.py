print("╔════════════════════════════════════════════════════╗")
print("║   AJX ULTIMATE: PRIORITY JSON FIX                  ║")
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
    [{"chapter_name
