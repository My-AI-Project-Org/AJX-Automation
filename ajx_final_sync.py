import os
import json
import time
import firebase_admin
from firebase_admin import credentials, db
from natsort import natsorted
from rich.console import Console
from rich.tree import Tree
from rich.panel import Panel

# --- GOOGLE DRIVE IMPORTS ---
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

console = Console()

# --- FIREBASE SETUP ---
if not firebase_admin._apps:
    try:
        key_json = os.environ.get("FIREBASE_SERVICE_KEY")
        if key_json:
            cred = credentials.Certificate(json.loads(key_json))
        else:
            cred = credentials.Certificate("serviceAccountKey.json")
        
        firebase_admin.initialize_app(cred, {
            'databaseURL': os.environ.get("FIREBASE_DB_URL")
        })
    except Exception as e:
        console.print(f"[red]⚠️ Firebase Error: {e}[/red]")

class DriveManager:
    """Handles Google Drive Folder Creation & Uploads"""
    def __init__(self, root_folder_name):
        self.scopes = ['https://www.googleapis.com/auth/drive']
        self.service = self._authenticate()
        self.root_folder_name = root_folder_name
        self.root_id = self._get_or_create_folder(root_folder_name, None)

    def _authenticate(self):
        try:
            # Service Account Credentials from Env
            creds_json = json.loads(os.environ.get("GDRIVE_CREDENTIALS"))
            creds = service_account.Credentials.from_service_account_info(
                creds_json, scopes=self.scopes)
            return build('drive', 'v3', credentials=creds)
        except Exception as e:
            console.print(f"[red]❌ Drive Auth Failed: {e}[/red]")
            return None

    def _get_or_create_folder(self, folder_name, parent_id):
        if not self.service: return None
        
        query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
        if parent_id:
            query += f" and '{parent_id}' in parents"
            
        results = self.service.files().list(q=query, fields="files(id, name)").execute()
        files = results.get('files', [])

        if files:
            return files[0]['id']
        else:
            # Create Folder
            meta = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [parent_id] if parent_id else []
            }
            file = self.service.files().create(body=meta, fields='id').execute()
            return file.get('id')

    def upload_file(self, file_path, file_name, parent_id):
        if not self.service: return
        
        # Check if file exists to update or skip
        query = f"name='{file_name}' and '{parent_id}' in parents and trashed=false"
        results = self.service.files().list(q=query).execute()
        files = results.get('files', [])

        media = MediaFileUpload(file_path, mimetype='application/json')
        
        if files:
            # Update existing
            file_id = files[0]['id']
            self.service.files().update(fileId=file_id, media_body=media).execute()
            # console.print(f"   [dim]🔄 Updated {file_name}[/dim]")
        else:
            # Create new
            meta = {'name': file_name, 'parents': [parent_id]}
            self.service.files().create(body=meta, media_body=media).execute()
            # console.print(f"   [green]⬆️ Uploaded {file_name}[/green]")

class AJXFinalSupervisor:
    def __init__(self, subject_name):
        self.subject = subject_name.upper()
        self.source_path = f"BACKUP/{self.subject}"
        self.monitor_ref = db.reference(f'Monitoring/{self.subject}/Finalizer')
        self.drive = None # Will init later

    def update_remote(self, status, message):
        try:
            self.monitor_ref.update({"status": status, "message": message})
        except: pass

    def verify_integrity(self):
        """Standard Integrity Check Logic (Same as before)"""
        console.print("[yellow]🕵️‍♂️ Auditing Data & Preparing Manifest...[/yellow]")
        seen_ids = set()
        collision_errors = []
        manifest = {}
        total_questions = 0

        for root, dirs, files in os.walk(self.source_path):
            files = natsorted(files)
            for file in files:
                if not file.endswith(".json"): continue
                
                file_path = os.path.join(root, file)
                rel_path = os.path.relpath(file_path, self.source_path)
                parts = rel_path.split(os.sep)
                
                if len(parts) >= 2:
                    unit = parts[-3] if len(parts) > 2 else "Uncategorized"
                    chapter = parts[-2]
                else: continue

                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        
                        # Integrity Checks
                        if not isinstance(data, list): continue
                        
                        local_min = float('inf')
                        local_max = float('-inf')
                        
                        for item in data:
                            q_id = item.get('id')
                            if not q_id: continue
                            if q_id in seen_ids:
                                collision_errors.append(f"ID {q_id} in {rel_path}")
                            else: seen_ids.add(q_id)
                            local_min = min(local_min, q_id)
                            local_max = max(local_max, q_id)
                        
                        # Manifest
                        if unit not in manifest: manifest[unit] = {}
                        if chapter not in manifest[unit]: 
                            manifest[unit][chapter] = {"files": [], "total_q": 0}
                        
                        manifest[unit][chapter]["total_q"] += len(data)
                        manifest[unit][chapter]["files"].append({
                            "file": file,
                            "range": f"{local_min}-{local_max}"
                        })
                        total_questions += len(data)
                        
                except: pass

        if collision_errors:
            console.print(Panel(f"[bold red]💥 COLLISIONS FOUND: {len(collision_errors)}[/bold red]"))
            return None
            
        return manifest, total_questions

    def sync_to_drive(self):
        """
        DEVOPS: Recursively mirrors the BACKUP folder to Google Drive.
        Creates Skeleton (Folders) + Uploads JSONs.
        """
        console.print(f"[bold cyan]☁️ Syncing to Google Drive: {self.subject}[/bold cyan]")
        
        # Init Drive Manager
        if not os.environ.get("GDRIVE_CREDENTIALS"):
            console.print("[red]❌ GDRIVE_CREDENTIALS missing. Skipping Drive Sync.[/red]")
            return

        self.drive = DriveManager(self.subject) # Creates Root Folder
        if not self.drive.root_id: return

        # Recursive Walk
        for root, dirs, files in os.walk(self.source_path):
            # 1. Determine relative path from backup root
            rel_path = os.path.relpath(root, self.source_path)
            
            if rel_path == ".":
                current_drive_id = self.drive.root_id
            else:
                # Traverse/Create folders in Drive to match local path
                # Ex: "01_Ancient/01_Stone_Age" -> Create Ancient -> Create Stone Age
                path_parts = rel_path.split(os.sep)
                parent_id = self.drive.root_id
                
                for folder in path_parts:
                    parent_id = self.drive._get_or_create_folder(folder, parent_id)
                current_drive_id = parent_id

            # 2. Upload Files in this folder
            files = natsorted(files)
            for file in files:
                if file.endswith(".json"):
                    local_path = os.path.join(root, file)
                    self.drive.upload_file(local_path, file, current_drive_id)
                    
        console.print("[green]✅ Google Drive Sync Complete![/green]")

    def run(self):
        console.print(Panel(f"[bold magenta]🏁 AJX SUPERVISOR: {self.subject}[/bold magenta]"))
        
        # 1. Verify
        result = self.verify_integrity()
        
        if result:
            manifest, total_q = result
            
            # 2. Upload Manifest to Firebase
            db.reference(f'Syllabus/{self.subject}/Manifest').set(manifest)
            db.reference(f'Syllabus/{self.subject}/Config').update({
                "status": "LIVE", 
                "total_questions": total_q
            })
            
            # 3. Drive Sync (RE-ADDED)
            self.sync_to_drive()
            
            console.print(Panel(f"[bold green]✅ SYSTEM LIVE! {total_q} Questions Secured.[/bold green]"))
        else:
            console.print("[red]❌ Sync Aborted.[/red]")

if __name__ == "__main__":
    subject = os.getenv("CURRENT_SUBJECT", "UPSI_HISTORY")
    if os.path.exists(f"BACKUP/{subject}"):
        AJXFinalSupervisor(subject).run()
    else:
        console.print("[red]❌ Backup Not Found[/red]")
