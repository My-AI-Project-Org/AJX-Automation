import os
import fitz
import json
import re
from natsort import natsorted
import google.generativeai as genai
import firebase_admin
from firebase_admin import credentials, db
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

# --- DEVOPS: LOADING SECRETS ---
GEMINI_KEYS = os.getenv("GEMINI_API_KEYS_LIST", "").split(",")
FIREBASE_KEY_STR = os.getenv("FIREBASE_SERVICE_KEY")
DB_URL = os.getenv("FIREBASE_DB_URL") 
GDRIVE_CREDS = os.getenv("GDRIVE_CREDENTIALS")

# Firebase Init
if not firebase_admin._apps:
    cred_dict = json.loads(FIREBASE_KEY_STR)
    cred = credentials.Certificate(cred_dict)
    firebase_admin.initialize_app(cred, {'databaseURL': DB_URL})

class AJXScout:
    def __init__(self):
        self.base_input = "AJX_Input"
        self.method_path = "Method_1_Vision"
        self.subject_id = ""
        self.master_prompt = ""

    # DSA: Natural Sorting for Chapters/Units
    def natural_sort_key(self, text):
        return [int(c) if c.isdigit() else c.lower() for c in re.split('([0-9]+)', text)]

    # DevOps: Dynamic Folder Discovery (e.g., UPSI_History)
    def discover_target(self):
        full_path = os.path.join(self.base_input, self.method_path)
        folders = [f for f in os.listdir(full_path) if os.path.isdir(os.path.join(full_path, f))]
        if folders:
            self.subject_id = folders[0].upper() # UPSI_HISTORY
            self.target_dir = os.path.join(full_path, folders[0])
            print(f"🎯 Target Detected: {self.subject_id}")
            return True
        return False

    def build_skeleton(self):
        # Mandatory Prompt Check
        prompt_file = os.path.join(self.target_dir, "master_prompt.txt")
        if not os.path.exists(prompt_file):
            raise Exception("❌ ERROR: master_prompt.txt is MANDATORY!")
        
        with open(prompt_file, 'r') as f: self.master_prompt = f.read()

        # Phase 1: Index Parsing (Gemini Vision)
        genai.configure(api_key=GEMINI_KEYS[0])
        model = genai.GenerativeModel('gemini-1.5-flash')
        
        index_pdf = os.path.join(self.target_dir, "index_page.pdf")
        doc = fitz.open(index_pdf)
        img_data = doc[0].get_pixmap().tobytes()
        
        prompt = "Analyze Index image. Return JSON: {'units': [{'name': '...', 'chapters': [{'name': '...', 'page': 0}]}]}"
        response = model.generate_content([prompt, {"mime_type": "image/jpeg", "data": img_data}])
        
        data = json.loads(response.text.strip('```json').strip('```'))

        # DSA: Push to Firebase Tree
        ref = db.reference(f'Syllabus/{self.subject_id}')
        ref.child("Config").set({"master_prompt": self.master_prompt})
        
        # Natural Sorting Units
        units = natsorted(data['units'], key=lambda x: self.natural_sort_key(x['name']))
        for unit in units:
            u_ref = ref.child(unit['name'])
            chapters = natsorted(unit['chapters'], key=lambda x: self.natural_sort_key(x['name']))
            for ch in chapters:
                u_ref.child(ch['name']).set({
                    "start_page": ch['page'],
                    "status": "SKELETON_READY"
                })
        print(f"✅ Phase 1 Complete: {self.subject_id} is LIVE on App.")

if __name__ == "__main__":
    scout = AJXScout()
    if scout.discover_target():
        scout.build_skeleton()
