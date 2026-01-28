print("╔════════════════════════════════════════════╗")
print("║   AJX PHASE 2: TOC DEEP SCAN & INDEXING    ║")
print("╚════════════════════════════════════════════╝")

import os
import json
import io
import time
import sys
import PIL.Image

# Google Libraries
from google.oauth2.credentials import Credentials 
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
import google.generativeai as genai

# --- CONFIGURATION ---
INPUT_ROOT = 'AJX_Phase1_Output'

# --- AUTHENTICATION ---
keys_json = os.environ.get("GEMINI_API_KEYS_LIST")
oauth_json = os.environ.get("GDRIVE_OAUTH_JSON")

if not keys_json or not oauth_json:
    print("❌ FATAL: Secrets missing. Script needs 'GEMINI_API_KEYS_LIST' and 'GDRIVE_OAUTH_JSON'.")
    exit()

# Setup Gemini
try:
    API_KEYS = json.loads(keys_json)
    if not isinstance(API_KEYS, list): API_KEYS = [keys_json]
    genai.configure(api_key=API_KEYS[0])
    model = genai.GenerativeModel('gemini-2.5-flash')
except:
    print("❌ Error setting up Gemini API.")
    exit()

# Setup Drive
try:
    token_info = json.loads(oauth_json)
    creds = Credentials.from_authorized_user_info(token_info, ['https://www.googleapis.com/auth/drive'])
    service = build('drive', 'v3', credentials=creds)
    print("✅ Authenticated with Drive.")
except Exception as e:
    print(f"❌ Drive Auth Error: {e}")
    exit()

# --- HELPER FUNCTIONS ---

def get_folder_id(folder_name, parent_id=None):
    query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    if parent_id:
        query += f" and '{parent_id}' in parents"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get('files', [])
    return files[0]['id'] if files else None

def get_latest_book_folder(parent_id):
    # Finds the most recently modified folder inside Output
    query = f"'{parent_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    results = service.files().list(q=query, orderBy='modifiedTime desc', pageSize=1, fields="files(id, name)").execute()
    files = results.get('files', [])
    if not files: return None, None
    return files[0]['id'], files[0]['name']

def get_first_chapter_images(book_folder_id, limit=20):
    # 1. Find the first subfolder (usually '01_Intro' or similar)
    query = f"'{book_folder_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    # Sort by name to get '01_...' first
    results = service.files().list(q=query, orderBy='name', pageSize=1, fields="files(id, name)").execute()
    subfolders = results.get('files', [])
    
    if not subfolders:
        print("❌ No subfolders found in book folder.")
        return []

    first_sub_id = subfolders[0]['id']
    print(f"📂 Scanning subfolder: {subfolders[0]['name']}")

    # 2. Get images from that folder
    img_query = f"'{first_sub_id}' in parents and mimeType = 'image/jpeg' and trashed = false"
    # Sort by name (page_001, page_002...)
    img_results = service.files().list(q=img_query, orderBy='name', pageSize=limit, fields="files(id, name)").execute()
    files = img_results.get('files', [])
    
    downloaded_images = []
    print(f"⬇️ Downloading first {len(files)} pages for analysis...")
    
    for f in files:
        request = service.files().get_media(fileId=f['id'])
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        
        fh.seek(0)
        img = PIL.Image.open(fh)
        downloaded_images.append(img)
    
    return downloaded_images

def analyze_toc(images):
    print("🧠 Analyzing Visual Hierarchy with Gemini...")
    prompt = """
    Analyze these images to extract the MASTER TABLE OF CONTENTS.
    I need a nested JSON structure with Page Ranges.
    
    OUTPUT FORMAT (Strict JSON):
    [
      {
        "chapter_name": "Name of Chapter",
        "start_page": 5,
        "subtopics": [
           {"name": "Subtopic 1", "start_page": 5},
           {"name": "Subtopic 2", "start_page": 8}
        ]
      }
    ]

    RULES:
    1. Look for the main 'Index' or 'Contents' pages.
    2. Ignore Preface/Copyright unless they are relevant.
    3. If you see page numbers like '5-10', extract start_page as 5.
    4. Return ONLY valid JSON. No markdown.
    """
    try:
        response = model.generate_content([prompt] + images)
        text = response.text.replace("```json", "").replace("```", "").strip()
        return json.loads(text)
    except Exception as e:
        print(f"❌ AI Analysis Failed: {e}")
        return []

def calculate_ranges(toc_data):
    # Simple logic to infer end_page from the next chapter's start_page
    print("Cc Calculating Page Ranges...")
    for i, chap in enumerate(toc_data):
        start = int(chap.get('start_page', 0))
        
        # Determine End Page
        if i < len(toc_data) - 1:
            next_start = int(toc_data[i+1].get('start_page', start))
            end = max(start, next_start - 1)
        else:
            end = start + 50 # Fallback for last chapter
        
        chap['start_page'] = start
        chap['end_page'] = end
        
        # Subtopic Logic
        subs = chap.get('subtopics', [])
        for j, sub in enumerate(subs):
            sub_start = int(sub.get('start_page', start))
            if j < len(subs) - 1:
                next_sub_start = int(subs[j+1].get('start_page', sub_start))
                sub_end = max(sub_start, next_sub_start - 1)
            else:
                sub_end = end # Last subtopic ends with chapter
            
            sub['start_page'] = sub_start
            sub['end_page'] = sub_end
            sub['range'] = f"{sub_start}-{sub_end}"

    return toc_data

def upload_json(folder_id, filename, data):
    file_path = "temp_index.json"
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    
    file_metadata = {'name': filename, 'parents': [folder_id]}
    media = MediaFileUpload(file_path, mimetype='application/json')
    
    print(f"⬆️ Uploading {filename} to Drive...")
    service.files().create(body=file_metadata, media_body=media).execute()
    print("✅ Upload Complete.")

def main():
    # 1. Locate Folders
    root_id = get_folder_id(INPUT_ROOT)
    if not root_id:
        print(f"❌ Could not find '{INPUT_ROOT}' in Drive.")
        return

    book_id, book_name = get_latest_book_folder(root_id)
    if not book_id:
        print("❌ No book folders found.")
        return

    print(f"📘 Processing Book: {book_name}")

    # 2. Get Images
    images = get_first_chapter_images(book_id)
    if not images: return

    # 3. Analyze
    toc_data = analyze_toc(images)
    if not toc_data:
        print("⚠️ AI could not find a Table of Contents.")
        # Create a dummy one so pipeline doesn't break
        toc_data = [{"chapter_name": "Full Book", "start_page": 1, "end_page": 999, "subtopics": []}]

    # 4. Refine Data
    final_data = calculate_ranges(toc_data)

    # 5. Upload
    json_filename = f"{book_name}_index.json"
    upload_json(book_id, json_filename, final_data)

    print("\n🎉 PHASE 2 COMPLETE: Index JSON created successfully.")

if __name__ == "__main__":
    main()
