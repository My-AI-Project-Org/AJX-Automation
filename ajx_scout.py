import os
import json
import math
import hashlib
import re
import firebase_admin
from firebase_admin import credentials, db
from natsort import natsorted
from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich.panel import Panel

console = Console()

# --- 1. FIREBASE INITIALIZATION ---
if not firebase_admin._apps:
    FIREBASE_KEY = json.loads(os.getenv("FIREBASE_SERVICE_KEY"))
    DB_URL = os.getenv("FIREBASE_DB_URL")
    cred = credentials.Certificate(FIREBASE_KEY)
    firebase_admin.initialize_app(cred, {'databaseURL': DB_URL})

class AJXScoutElite:
    def __init__(self):
        self.method_dirs = {"METHOD_1": 1, "METHOD_2": 2}
        self.cache_file = ".ajx_cache.json"
        self.cache = self.load_cache()
        self.monitor_ref = None

    def load_cache(self):
        if os.path.exists(self.cache_file):
            with open(self.cache_file, 'r') as f: return json.load(f)
        return {}

    # --- DSA: NATURAL SORTING LOGIC (From Old Script) ---
    def natural_sort_key(self, text):
        return [int(c) if c.isdigit() else c.lower() for c in re.split('([0-9]+)', text)]

    def get_md5(self, folder_path):
        """DevOps: O(1) Folder Hashing for Deduplication"""
        hash_md5 = hashlib.md5()
        for root, dirs, files in os.walk(folder_path):
            for names in natsorted(files): # Ensure hash consistency with sorting
                filepath = os.path.join(root, names)
                with open(filepath, 'rb') as f:
                    for chunk in iter(lambda: f.read(4096), b""):
                        hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def update_remote_monitor(self, subject, status, progress, current_task=""):
        """DevOps: Real-time Terminal Sync to Firebase Console"""
        if not self.monitor_ref:
            self.monitor_ref = db.reference(f'Monitoring/{subject}/Scout')
        self.monitor_ref.update({
            "status": status,
            "progress": f"{progress}%",
            "current_task": current_task,
            "last_ping": "2026-02-01 14:15:00"
        })

    def sync_skeleton_dfs(self, ref, data_tree):
        """DSA: Depth-First Search Recursive N-ary Tree Construction"""
        if isinstance(data_tree, list):
            # Apply Natural Sort to topics before syncing
            sorted_data = natsorted(data_tree, key=lambda x: self.natural_sort_key(str(x.get('topic', ''))))
            for item in sorted_data:
                topic = str(item.get('topic', 'Main')).replace(".", "_")
                if not ref.child(topic).get():
                    ref.child(topic).set({
                        "status": "SKELETON_READY", 
                        "mcq_count": 0,
                        "method_hint": "DFS_BRANCH"
                    })
        elif isinstance(data_tree, dict):
            # Natural sort the keys of the dictionary
            sorted_keys = natsorted(data_tree.keys(), key=self.natural_sort_key)
            for key in sorted_keys:
                clean_key = key.replace(".", "_")
                self.sync_skeleton_dfs(ref.child(clean_key), data_tree[key])

    def scan(self):
        console.print(Panel("[bold cyan]🛰️ AJX SCOUT ULTIMATE v6.0[/bold cyan]\n[dim]Natural Sort | MD5 Dedupe | DFS Mirror | Remote Live[/dim]"))
        
        for m_dir, m_id in self.method_dirs.items():
            if not os.path.exists(m_dir): continue
            
            # Natural sort subjects
            for subject in natsorted(os.listdir(m_dir)):
                sub_path = os.path.join(m_dir, subject)
                if not os.path.isdir(sub_path): continue
                
                sub_upper = subject.upper() # Target Detected: UPSI_HISTORY
                current_hash = self.get_md5(sub_path)

                # 1. Deduplication Logic (MD5)
                if self.cache.get(sub_upper) == current_hash:
                    console.print(f"[yellow]⏩ {sub_upper} Verified (Hash Match). Skipping...[/yellow]")
                    continue

                # 2. Setup Colorful Live Dashboard
                table = Table(show_header=True, header_style="bold magenta", expand=True)
                table.add_column("🚀 Phase", width=25)
                table.add_column("📊 Status", justify="center")
                table.add_column("🛰️ Firebase Live", justify="right")

                with Live(table, refresh_per_second=4):
                    # --- Step 1: Verification ---
                    table.add_row("Verification", "[yellow]Scanning Files...[/yellow]", "⏳")
                    files = {f.upper(): f for f in os.listdir(sub_path)}
                    self.update_remote_monitor(sub_upper, "VERIFYING", 10, "Validating folder structure")
                    
                    if "MASTER_PROMPT.TXT" not in files:
                        table.add_row("Verification", "[red]Failed (Prompt Missing)[/red]", "❌")
                        continue
                    table.add_row("Verification", "[green]Verified ✅[/green]", "📶")

                    # --- Step 2: Skeleton DFS Sync ---
                    table.add_row("DFS N-ary Tree", "[yellow]Syncing Tree...[/yellow]", "⏳")
                    self.update_remote_monitor(sub_upper, "SKELETON_SYNC", 40, "Building Firebase DFS Tree")
                    
                    with open(os.path.join(sub_path, files["MASTER_PROMPT.TXT"]), 'r') as f:
                        prompt = f.read()
                    
                    syllabus = []
                    if m_id == 1: 
                        # ✅ REAL LOGIC: INDEX.PDF Read karega
                        index_file_path = os.path.join(sub_path, "INDEX.pdf")
                        
                        if os.path.exists(index_file_path):
                            try:
                                doc = fitz.open(index_file_path)
                                for page in doc:
                                    text = page.get_text()
                                    # Har line ko ek Topic maan lete hain
                                    lines = text.split('\n')
                                    for line in lines:
                                        clean_line = line.strip()
                                        # Sirf tab add karo agar line mein kuch likha ho aur wo page number na ho
                                        if clean_line and len(clean_line) > 3: 
                                            syllabus.append({"topic": clean_line, "chapter": "PDF_AUTO"})
                                            
                                console.print(f"[green]📖 Read {len(syllabus)} topics from INDEX.pdf[/green]")
                            except Exception as e:
                                console.print(f"[red]❌ Error reading PDF: {e}[/red]")
                                # Error aaya toh fallback dummy data
                                syllabus = [{"topic": "ERROR_READING_PDF", "chapter": "ERROR"}]
                        else:
                            console.print("[red]❌ INDEX.pdf file missing in folder![/red]")
                            continue

                    else:
                        # Method 2 (JSON Logic - Same as before)
                        with open(os.path.join(sub_path, files["SYLLABUS_DB.JSON"]), 'r') as f:
                            syllabus = json.load(f)

                    # Firebase Tree Build
                    ref = db.reference(f'Syllabus/{sub_upper}')
                    ref.child("Config").update({"prompt": prompt, "hash": current_hash})
                    self.sync_skeleton_dfs(ref.child("Data"), syllabus)
                    table.add_row("DFS N-ary Tree", "[green]Natural Sorted 🌳[/green]", "📶")

                    # --- Step 3: Matrix Splitting ---
                    table.add_row("Matrix Partitioning", "[yellow]Creating Tasks...[/yellow]", "⏳")
                    self.update_remote_monitor(sub_upper, "PARTITIONING", 85, "Splitting topics for 3 Workers")
                    
                    # Ensure syllabus is natural sorted before splitting
                    sorted_syllabus = natsorted(syllabus, key=lambda x: self.natural_sort_key(str(x.get('topic',''))))
                    
                    chunk = math.ceil(len(sorted_syllabus) / 3)
                    for i in range(3):
                        batch = sorted_syllabus[i*chunk:(i+1)*chunk]
                        with open(f"WORKER_TASK_{i+1}.JSON", 'w') as f:
                            json.dump({
                                "SUBJECT": sub_upper, 
                                "METHOD": m_id, 
                                "SOURCE": sub_path, 
                                "BATCH": batch, 
                                "MASTER_PROMPT": prompt,
                                "WORKER_ID": i+1
                            }, f, indent=4)
                    
                    table.add_row("Matrix Partitioning", "[green]3 Workers Ready 📂[/green]", "📶")

                # Update Cache & Final Status
                self.cache[sub_upper] = current_hash
                with open(self.cache_file, 'w') as f: json.dump(self.cache, f)
                self.update_remote_monitor(sub_upper, "SCOUT_FINISHED", 100, "Ready for Matrix Generation")
                console.print(Panel(f"[bold green]✅ SCOUT SUCCESS: {sub_upper} IS LIVE[/bold green]"))

if __name__ == "__main__":
    AJXScoutElite().scan()
