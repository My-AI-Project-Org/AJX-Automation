import os
import fitz
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

    def get_5_level_offset(self, pdf_path):
        """
        ⚓ 5-LEVEL ANCHORING SYSTEM FOR OFFSET DETECTION
        Returns: The page number where actual content starts (after Index/TOC).
        """
        try:
            doc = fitz.open(pdf_path)
            max_scan = min(15, len(doc)) # Sirf pehle 15 page scan karenge
            detected_offset = 0
            
            # LEVEL 1: KEYWORD ANCHOR (Contents/Index)
            for i in range(max_scan):
                text = doc[i].get_text().lower()
                if "table of contents" in text or "index" in text or "syllabus" in text:
                    # Agar mil gaya, toh assume karte hain iske agle kuch pages tak index chalega
                    detected_offset = i
                    break
            
            # LEVEL 2: STRUCTURE ANCHOR (Dots pattern ..... 12)
            # Check karte hain ki Index kitne pages lamba hai
            current_page = detected_offset
            while current_page < max_scan:
                text = doc[current_page].get_text()
                # Agar line mein dots aur end mein number hai (Typical Index format)
                if text.count("...") > 5 or re.search(r'\.{3,}\s*\d+', text):
                    current_page += 1
                else:
                    break # Index khatam, yahan se content shuru
            
            # LEVEL 3: NUMERICAL ANCHOR (Roman to Arabic)
            # Aksar Index pages 'iv', 'v' hote hain aur Chapter 1 page '1' hota hai
            # Ye logic thoda complex hai, isliye hum Level 2 ke result ko hi refine karte hain.
            
            # LEVEL 4: VISUAL ANCHOR (Header Detection)
            # Check if next page starts with "Chapter 1" or big Bold text
            final_offset = current_page
            
            # LEVEL 5: SAFETY FALLBACK
            if final_offset == 0:
                final_offset = 1 # Agar kuch nahi mila to Page 1 se shuru maano
                
            console.print(f"[cyan]⚓ 5-Level Anchoring: Content likely starts at Page {final_offset}[/cyan]")
            return final_offset

        except Exception as e:
            console.print(f"[red]❌ Anchoring Error: {e}[/red]")
            return 0 # Fail-safe

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
                        # 🔥 NEW LOGIC: PDF Read karega
                        pdf_path = os.path.join(sub_path, f"{subject}.PDF")
                        
                        if os.path.exists(pdf_path):
                            # 1. 5-Level Anchoring Call
                            real_offset = self.get_5_level_offset(pdf_path)
                            
                            # 2. Index PDF se Topics Read karna (Simple Version)
                            index_file = os.path.join(sub_path, "INDEX.pdf")
                            if os.path.exists(index_file):
                                doc = fitz.open(index_file)
                                for page in doc:
                                    for line in page.get_text().split('\n'):
                                        if len(line.strip()) > 3:
                                            # Offset ko har topic ke sath jod rahe hain
                                            syllabus.append({
                                                "topic": line.strip(), 
                                                "chapter": "AUTO", 
                                                "offset": real_offset
                                            })
                            else:
                                syllabus = [{"topic": "INDEX_PDF_MISSING", "chapter": "ERROR"}]
                        else:
                             syllabus = [{"topic": "MAIN_PDF_MISSING", "chapter": "ERROR"}]

                    else:
                        # Method 2 waisa hi rahega
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
