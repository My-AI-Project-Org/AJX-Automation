print("╔════════════════════════════════════════════════════╗")
print("║   AJX PHASE 2: CLOUD BRAIN (DRIVE CONNECTED)       ║")
print("╚════════════════════════════════════════════════════╝")

import os
import json
import time
import sys
import io
import urllib.request
import urllib.parse
from collections import deque

# Google Libs (Ye zaroori hain Cloud ke liye)
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

# --- CONFIG ---
OUTPUT_FOLDER_NAME = 'AJX_Phase1_Output' # Drive Folder Name to search
CONFIG_FILE_NAME = 'config.json'
PROMPT_FILE_NAME = 'master_prompt.txt'

# --- 🟢 LIVE TELEGRAM TERMINAL ---
class TelegramTerminal:
    def __init__(self):
        self.token = os.environ.get("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.environ.get("TELEGRAM_CHAT_ID")
        self.message_id = None
        self.last_update_time = 0
        self.log_buffer = deque(maxlen=10)
        self.current_progress = 0
        self.current_status = "Initializing..."

    def start(self):
        if not self.token: return
        self.message_id = self._send_new("<b>💻 AJX PHASE 2 (CLOUD)</b>\nConnecting...")

    def log_stream(self, msg):
        clean_msg = str(msg).replace("<", "&lt;").replace(">", "&gt;") 
        self.log_buffer.append(f"> {clean_msg}")
        self._refresh_display()

    def update_progress(self, percent, status):
        self.current_progress = percent
        self.current_status = status
        self._refresh_display()

    def _refresh_display(self):
        if time.time() - self.last_update_time < 1.5 and self.current_progress < 100: return
        if not self.token or not self.message_id: return
        logs_text = "\n".join(self.log_buffer)
        bar_len = 10
        filled = int(bar_len * self.current_progress / 100)
        bar = "█" * filled + "░" * (bar_len - filled)
        text = (f"<b>💻 AJX PHASE 2 (CLOUD)</b>\n<code>{logs_text}</code>\n"
                f"━━━━━━━━━━━━━━━━━━\n<b>{self.current_status}</b>\n"
                f"<code>[{bar}] {self.current_progress}%</code>")
        self._edit_msg(text)
        self.last_update_time = time.time()

    def _send_new(self, text):
        try:
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            data = urllib.parse.urlencode({"chat_id": self.chat_id, "text": text, "parse_mode": "HTML"}).encode()
            with urllib.request.urlopen(urllib.request.Request(url, data=data)) as response:
                return json.loads(response.read())['result']['message_id']
        except: return None

    def _edit_msg(self, text):
        try:
            url = f"https://api.telegram.org/bot{self.token}/editMessageText"
            data = urllib.parse.urlencode({"chat_id": self.chat_id, "message_id": self.message_id, "text": text, "parse_mode": "HTML"}).encode()
            urllib.request.urlopen(urllib.request.Request(url, data=data))
        except: pass

terminal = TelegramTerminal()

def log(msg):
    print(msg)
    sys.stdout.flush()
    terminal.log_stream(msg)

# --- ☁️ DRIVE LOGIC (Yeh Naya Hai) ---
def get_drive_service():
    oauth_json = os.environ.get("GDRIVE_OAUTH_JSON")
    if not oauth_json:
        log("❌ Secrets Missing: GDRIVE_OAUTH_JSON")
        return None
    try:
        token_info = json.loads(oauth_json)
        creds = Credentials.from_authorized_user_info(token_info, ['https://www.googleapis.com/auth/drive'])
        return build('drive', 'v3', credentials=creds)
    except Exception as e:
        log(f"❌ Auth Error: {e}")
        return None

def find_latest_index_on_drive(service):
    # 1. Output Folder dhoondo
    query = f"name = '{OUTPUT_FOLDER_NAME}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    results = service.files().list(q=query, fields="files(id)").execute()
    folders = results.get('files', [])
    
    if not folders: return None, None
    out_folder_id = folders[0]['id']

    # 2. Uske andar latest Book Folder dhoondo
    query = f"'{out_folder_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    results = service.files().list(q=query, orderBy='createdTime desc', pageSize=1, fields="files(id, name)").execute()
    book_folders = results.get('files', [])
    
    if not book_folders: return None, None
    book_id = book_folders[0]['id']

    # 3. Uske andar _index.json dhoondo
    query = f"'{book_id}' in parents and name contains '_index.json' and trashed = false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get('files', [])
    
    if not files: return None, None
    return files[0], book_id 

def download_file_content(service, file_id):
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done: status, done = downloader.next_chunk()
    fh.seek(0)
    return fh.read().decode('utf-8')

def upload_config_to_drive(service, parent_id, config_dict):
    # Local save for Artifacts
    with open(CONFIG_FILE_NAME, 'w') as f:
        json.dump(config_dict, f, indent=4)
    
    # Upload to Drive
    file_metadata = {'name': CONFIG_FILE_NAME, 'parents': [parent_id]}
    media = MediaFileUpload(CONFIG_FILE_NAME, mimetype='application/json')
    service.files().create(body=file_metadata, media_body=media).execute()

# --- LOCAL LOGIC (Prompt Local hi rahega) ---
def get_prompt_content():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    paths = [
        os.path.join(script_dir, PROMPT_FILE_NAME),
        os.path.join(os.getcwd(), PROMPT_FILE_NAME),
        os.path.join(os.path.dirname(script_dir), PROMPT_FILE_NAME)
    ]
    for p in paths:
        if os.path.exists(p):
            log(f"📄 Prompt Found: {os.path.basename(p)}")
            try: return open(p, 'r', encoding='utf-8').read()
            except: pass
    return None

def main():
    terminal.start()
    
    # 1. Drive Connect Karo
    terminal.update_progress(10, "Auth Google Drive...")
    service = get_drive_service()
    if not service: return

    # 2. Prompt Load Karo
    terminal.update_progress(20, "Loading Prompt...")
    master_prompt = get_prompt_content()
    if not master_prompt:
        log(f"❌ Error: '{PROMPT_FILE_NAME}' missing.")
        terminal.update_progress(0, "FILE NAME ERROR")
        return

    # 3. Cloud par Index Dhoondo
    terminal.update_progress(40, "Searching Cloud...")
    log("☁️ Searching Drive for latest Index...")
    index_file, book_folder_id = find_latest_index_on_drive(service)
    
    if not index_file:
        log("❌ Error: No Index found on Drive. Phase 1 shayad fail hua.")
        terminal.update_progress(0, "INDEX MISSING")
        return

    log(f"✅ Found Cloud Index: {index_file['name']}")
    
    # 4. Index Download & Process
    terminal.update_progress(60, "Downloading Index...")
    json_content = download_file_content(service, index_file['id'])
    index_data = json.loads(json_content)
    
    # 5. Config Banao
    terminal.update_progress(80, "Generating Config...")
    book_name = index_file['name'].replace("_index.json", "")
    
    config = {
        "book_id": str(int(time.time())),
        "book_name": book_name,
        "exam_target": "General_Competition", 
        "subject": "General_Studies",       
        "total_chapters": len(index_data),
        "prompt_template": master_prompt
    }
    
    # 6. Config Wapas Upload Karo
    terminal.update_progress(90, "Uploading Config...")
    upload_config_to_drive(service, book_folder_id, config)
    
    log(f"🎉 Config uploaded to Drive!")
    terminal.update_progress(100, "✅ PHASE 2 DONE")
    terminal.log_stream("Ready for Phase 3")

if __name__ == "__main__":
    main()
