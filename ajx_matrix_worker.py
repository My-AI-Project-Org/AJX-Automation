import os
import json
import zstandard as zstd
import google.generativeai as genai
from concurrent.futures import ThreadPoolExecutor
import firebase_admin
from firebase_admin import credentials, db
import re
import fitz  # PyMuPDF
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeRemainingColumn

console = Console()

# --- FIREBASE SETUP (DevOps logic for VM) ---
if not firebase_admin._apps:
    FIREBASE_KEY = json.loads(os.getenv("FIREBASE_SERVICE_KEY"))
    DB_URL = os.getenv("FIREBASE_DB_URL")
    cred = credentials.Certificate(FIREBASE_KEY)
    firebase_admin.initialize_app(cred, {'databaseURL': DB_URL})

class EliteMatrixWorker:
    def __init__(self, task_file):
        with open(task_file, 'r') as f:
            self.task_data = json.load(f)
        self.worker_id = os.getenv('WORKER_ID', '1')
        # DevOps: Loading 8 dedicated keys for this Worker
        self.keys = os.getenv(f"KEYS_VM_{self.worker_id}").split(",")
        self.compressor = zstd.ZstdCompressor(level=3)

    def recursive_repair(self, raw_text, attempt=1):
        """DSA: Recursive Logic to fix AI JSON formatting errors"""
        try:
            clean_text = re.sub(r'```json|```', '', raw_text).strip()
            return json.loads(clean_text)
        except Exception:
            if attempt < 3:
                console.print(f"[yellow]⚠️ Attempt {attempt}: Repairing JSON syntax...[/yellow]")
                fixed_text = self.fix_json_syntax(clean_text)
                return self.recursive_repair(fixed_text, attempt + 1)
            return None

    def fix_json_syntax(self, text):
        """Simple Regex Repair for common AI trailing/bracket mistakes"""
        text = text.strip()
        if not text.endswith("]"): text += "]"
        if not text.startswith("["): text = "[" + text
        return text

    def fire_engine(self, topic_data, progress, task_id):
        """DevOps: 8-Key Threaded Engine firing logic"""
        # Load Balance across 8 keys
        key_index = (topic_data['index']) % len(self.keys)
        genai.configure(api_key=self.keys[key_index])
        model = genai.GenerativeModel('gemini-2.0-flash')

        mcqs = None
        # Switch between Method 1 (Vision) and Method 2 (Text)
        if self.task_data['METHOD'] == 1:
            mcqs = self.process_method_1(model, topic_data)
        else:
            mcqs = self.process_method_2(model, topic_data)

        if mcqs:
            self.pack_and_sync(topic_data['topic'], mcqs)
            # Live Progress Update
            progress.advance(task_id)
            console.print(f"[green]✅ '{topic_data['topic']}' live on Firebase & Local Backup[/green]")

    def process_method_1(self, model, topic_data):
        """Method 1: Gemini Vision Logic (PDF -> Image -> MCQ)"""
        try:
            pdf_path = os.path.join(self.task_data['SOURCE_PATH'], f"{self.task_data['SUBJECT']}.PDF")
            doc = fitz.open(pdf_path)
            page_num = topic_data.get('page', 0) 
            page = doc[page_num]
            pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
            img_data = pix.tobytes("jpeg")

            # Only uses MASTER_PROMPT (contains 40-60 MCQ range instruction)
            prompt = self.task_data.get('MASTER_PROMPT')
            response = model.generate_content([prompt, {"mime_type": "image/jpeg", "data": img_data}])
            return self.recursive_repair(response.text)
        except Exception as e:
            console.print(f"[red]Method 1 Error: {e}[/red]")
            return None

    def process_method_2(self, model, topic_data):
        """Method 2: Gemini Brain Logic (Prompt -> MCQ)"""
        prompt = f"Topic: {topic_data['topic']}\nInstructions: {self.task_data.get('MASTER_PROMPT')}"
        try:
            response = model.generate_content(prompt)
            return self.recursive_repair(response.text)
        except Exception as e:
            console.print(f"[red]Method 2 Error: {e}[/red]")
            return None

    def pack_and_sync(self, topic, data):
        """DSA: Protobuf Simulation + Zstd Compression"""
        # 1. Firebase Live Sync (Binary Hex)
        packed_str = json.dumps(data)
        compressed = self.compressor.compress(packed_str.encode('utf-8'))
        
        topic_name = topic.get('topic', topic) if isinstance(topic, dict) else topic
        topic_node = topic_name.replace(".", "_")
        
        db.reference(f"Syllabus/{self.task_data['SUBJECT']}/{topic_node}").update({
            "status": "COMPLETED",
            "payload": compressed.hex(),
            "mcq_count": len(data)
        })

        # 2. Local Backup (Will be synced to G-Drive in Phase 3)
        backup_dir = f"BACKUP/{self.task_data['SUBJECT']}"
        os.makedirs(backup_dir, exist_ok=True)
        with open(f"{backup_dir}/{topic_node}.json", "w") as f:
            json.dump(data, f, indent=4)

    def start_matrix(self):
        """Orchestrator: Multi-threading logic"""
        batch = self.task_data['BATCH']
        topics_with_index = [{"index": i, "topic": t} for i, t in enumerate(batch)]
        
        console.print(Panel(f"[bold cyan]🚀 WORKER {self.worker_id} MATRIX ACTIVE[/bold cyan]\n[dim]Subject: {self.task_data['SUBJECT']}[/dim]"))
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(bar_width=40, complete_style="green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console
        ) as progress:
            task_id = progress.add_task(f"Firing Topics...", total=len(batch))
            
            # Using 8 Threads to match 8 API Keys per Worker
            with ThreadPoolExecutor(max_workers=8) as executor:
                executor.map(lambda t: self.fire_engine(t, progress, task_id), topics_with_index)

from rich.panel import Panel
if __name__ == "__main__":
    t_file = f"WORKER_TASK_{os.getenv('WORKER_ID', '1')}.JSON"
    if os.path.exists(t_file):
        EliteMatrixWorker(t_file).start_matrix()
    else:
        console.print("[red]❌ Task File Not Found. Check Phase 1 Output.[/red]")
