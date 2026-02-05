import os
import json
import time
import re
import zstandard as zstd
import google.generativeai as genai
from concurrent.futures import ThreadPoolExecutor
import firebase_admin
from firebase_admin import credentials, db
from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.panel import Panel
import base64
import hashlib
import PIL.Image

console = Console()

# --- FIREBASE SETUP (DO NOT TOUCH) ---
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

class EliteMatrixWorker:
    def __init__(self, task_file):
        with open(task_file, 'r', encoding='utf-8') as f:
            self.task_data = json.load(f)
        
        self.worker_id = self.task_data.get('WORKER_ID', '1')
        self.subject = self.task_data['SUBJECT']
        self.method_type = self.task_data.get('METHOD_TYPE', 'METHOD_1')
        
        # Load Keys from Env (Comma Separated)
        self.keys = os.getenv(f"KEYS_VM_{self.worker_id}", "").split(",")
        if not self.keys or self.keys == [""]:
            console.print("[red]❌ No API Keys found in Environment![/red]")
            exit()

        self.compressor = zstd.ZstdCompressor(level=3)
        
        # UI Tracking
        self.completed_count = 0
        self.total_topics = len(self.task_data['BATCH_DATA'])
        self.thread_status = {f"T{i+1}": {"key": "...", "topic": "Idle...", "state": "WAITING"} for i in range(8)}

    # --- JSON REPAIR SYSTEM (DO NOT TOUCH) ---
    def recursive_repair(self, raw_text, attempt=1):
        try:
            clean_text = re.sub(r'```json|```', '', raw_text).strip()
            # Remove any non-json preamble
            if "[" in clean_text: clean_text = "[" + clean_text.split("[", 1)[1]
            if "]" in clean_text: clean_text = clean_text.rsplit("]", 1)[0] + "]"
            return json.loads(clean_text)
        except Exception:
            if attempt < 3:
                fixed_text = self.fix_json_syntax(clean_text)
                return self.recursive_repair(fixed_text, attempt + 1)
            return None

    def fix_json_syntax(self, text):
        text = text.strip()
        if not text.endswith("]"): text += "]"
        if not text.startswith("["): text = "[" + text
        return text

    # --- UI DASHBOARD (DO NOT TOUCH) ---
    def make_dashboard(self):
        pct = (self.completed_count / self.total_topics) * 100 if self.total_topics > 0 else 0
        prog_bar = f"🔥 PROGRESS: [[green]{'█' * int(pct/10)}{'░' * (10-int(pct/10))}[/green]] {pct:.1f}% ({self.completed_count}/{self.total_topics})"
        
        table = Table(box=None, expand=True)
        table.add_column("🧵 Thread", style="magenta")
        table.add_column("🔑 Key", style="cyan")
        table.add_column("📂 Target", style="white")
        table.add_column("⚡ Status", justify="right")

        for t_id, info in self.thread_status.items():
            color = "green" if "DONE" in info['state'] else "yellow"
            if "ERROR" in info['state']: color = "red"
            table.add_row(t_id, info['key'][-4:], info['topic'], f"[{color}]{info['state']}[/{color}]")

        dashboard = Panel(
            f"🚀 [bold cyan]AJX MATRIX WORKER #{self.worker_id}[/bold cyan] | [green]🟢 ONLINE[/green]\n"
            f"📊 SUBJECT: {self.subject} | 🛠️ MODE: {self.method_type}\n\n"
            f"{prog_bar}\n"
            f"─────────────────────────────────────────────────────────────────────────────\n"
            + str(console.render_str(str(table))) +
            f"\n─────────────────────────────────────────────────────────────────────────────\n"
            f"📡 SYNC: Firebase [LIVE] | Compression [ZSTD]",
            border_style="bright_blue"
        )
        return dashboard

    # ==========================================
    # 🧠 NEW: PROCESS LOGIC (UPDATED)
    # ==========================================

    def process_task(self, model, task_item):
        """Decides whether to use Image Mode or Text Mode based on Scout Data"""
        
        # 1. PREPARE PROMPT (Inject Formatting Rules)
        base_prompt = self.task_data.get('MASTER_PROMPT', 'Generate MCQs')
        
        # Add ID Instructions (Crucial for Display)
        # We ask LLM to start from 1, we will map Global ID later in Python
        prompt_instructions = (
            f"\n\nContext Topic: {task_item['chapter_name']}\n"
            f"IMPORTANT: Output pure JSON list of objects. "
            f"Each object must have 'question', 'options' (list), 'answer', 'explanation'. "
            f"Start serial numbering from {task_item.get('display_num_start', 1)}."
        )
        
        final_prompt = base_prompt + prompt_instructions

        # 2. CHECK MODE (Image vs Text)
        images = task_item.get('images', [])
        
        try:
            response = None
            
            # --- METHOD 1: IMAGE MODE ---
            if images: 
                # Load Images from Disk (Scout already extracted them)
                img_objects = []
                for img_file in images:
                    img_path = os.path.join(task_item['folder_path'], img_file)
                    if os.path.exists(img_path):
                        img_objects.append(PIL.Image.open(img_path))
                
                if not img_objects:
                    return None # Images missing?
                
                # Send to Gemini (Prompt + Images)
                content = [final_prompt] + img_objects
                response = model.generate_content(content)

            # --- METHOD 2: TEXT/TOPIC MODE ---
            else:
                response = model.generate_content(final_prompt)
            
            # 3. REPAIR JSON
            data = self.recursive_repair(response.text)
            
            # 4. 🛡️ GLOBAL ID INJECTION (The Safety Lock)
            # LLM might mess up IDs, so we overwrite them with Scout's Math.
            if data and isinstance(data, list):
                current_global_id = task_item['global_id_start']
                current_display_num = task_item.get('display_num_start', 1)
                
                for q in data:
                    q['id'] = current_global_id         # DB Unique Key
                    q['display_num'] = current_display_num # User Friendly Key
                    
                    current_global_id += 1
                    current_display_num += 1
                return data
            
            return None

        except Exception as e:
            # console.print(f"Gen Error: {e}")
            return None

    # ==========================================
    # 📦 SYNC LOGIC (UPDATED PATHS)
    # ==========================================

    def pack_and_sync(self, task_item, data):
        """Syncs data to Firebase with correct Unit/Chapter hierarchy"""
        
        # 1. Compress & Encrypt (Standard)
        packed_str = json.dumps(data)
        compressed_bytes = self.compressor.compress(packed_str.encode('utf-8'))
        payload_base64 = base64.b64encode(compressed_bytes).decode('utf-8')
        md5_hash = hashlib.md5(payload_base64.encode()).hexdigest()

        # 2. Clean Path Construction
        # Scout ensures names are safe, but we double check
        unit_node = task_item['unit_name'].replace(".", "").replace("/", "_")
        chap_node = task_item['chapter_name'].replace(".", "").replace("/", "_")
        
        # Path: Syllabus/History/Data/01_Ancient/01_Stone_Age
        # Note: 'folder_path' from Scout usually contains the hierarchy
        # We can also use the structure directly if Scout passed raw names
        
        # Construct path using the specific hierarchy provided by Scout's folder structure
        # task_item['folder_path'] example: "EXTRACTED_ASSETS/01_Ancient/01_Stone_Age"
        path_parts = task_item['folder_path'].split(os.sep)
        if len(path_parts) >= 3:
            unit_folder = path_parts[-2]
            chap_folder = path_parts[-1]
        else:
            # Fallback
            unit_folder = "Uncategorized"
            chap_folder = chap_node

        # Firebase Update
        ref_path = f"Syllabus/{self.subject}/Data/{unit_folder}/{chap_folder}"
        
        db.reference(ref_path).update({
            "status": "COMPLETED",
            "payload": payload_base64,
            "hash": md5_hash,
            "count": len(data),
            "global_id_start": task_item['global_id_start']
        })

    # ==========================================
    # ⚙️ ENGINE (THREAD LOGIC)
    # ==========================================

    def fire_engine(self, task_item, index):
        t_id = f"T{(index % 8) + 1}"
        key_index = index % len(self.keys)
        
        chap_name = task_item['chapter_name'][:20]
        self.thread_status[t_id].update({"key": f"...{self.keys[key_index][-4:]}", "topic": chap_name, "state": "GENERATING..."})
        
        try:
            genai.configure(api_key=self.keys[key_index])
            model = genai.GenerativeModel('gemini-2.0-flash')
            
            # CALL PROCESSOR
            mcqs = self.process_task(model, task_item)

            if mcqs:
                self.thread_status[t_id]['state'] = "SYNCING..."
                self.pack_and_sync(task_item, mcqs)
                self.completed_count += 1
                self.thread_status[t_id]['state'] = "DONE 💎"
            else:
                self.thread_status[t_id]['state'] = "RETRY/FAIL ⚠️"
                
        except Exception as e:
            self.thread_status[t_id]['state'] = "ERROR 🔴"

    def start_matrix(self):
        batch = self.task_data['BATCH_DATA'] # Correct Key from Scout
        
        with Live(self.make_dashboard(), refresh_per_second=4) as live:
            with ThreadPoolExecutor(max_workers=8) as executor:
                # Map futures
                futures = [executor.submit(self.fire_engine, item, i) for i, item in enumerate(batch)]
                
                while any(not f.done() for f in futures):
                    live.update(self.make_dashboard())
                    time.sleep(0.5)
                
                live.update(self.make_dashboard())

        console.print(Panel(f"[bold green]✅ WORKER {self.worker_id} FINISHED![/bold green]"))

if __name__ == "__main__":
    # Auto-detect task file based on Environment Variable or Loop
    w_id = os.getenv('WORKER_ID', '1')
    t_file = f"WORKER_TASK_{w_id}.json"
    
    if os.path.exists(t_file):
        EliteMatrixWorker(t_file).start_matrix()
    else:
        console.print(f"[red]❌ Task File '{t_file}' Not Found.[/red]")
