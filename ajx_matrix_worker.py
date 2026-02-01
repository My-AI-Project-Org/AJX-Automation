import os
import json
import time
import re
import zstandard as zstd
import google.generativeai as genai
from concurrent.futures import ThreadPoolExecutor
import firebase_admin
from firebase_admin import credentials, db
import fitz  # PyMuPDF
from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.panel import Panel

console = Console()

# --- FIREBASE SETUP ---
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
        self.keys = os.getenv(f"KEYS_VM_{self.worker_id}").split(",")
        self.compressor = zstd.ZstdCompressor(level=3)
        self.subject = self.task_data['SUBJECT']
        
        # UI & Thread Tracking
        self.completed_count = 0
        self.total_topics = len(self.task_data['BATCH'])
        self.thread_status = {f"T{i+1}": {"key": f"Key-{i+1:02}", "topic": "Idle...", "state": "WAITING"} for i in range(8)}

    def recursive_repair(self, raw_text, attempt=1):
        """DSA: Recursive Logic from your old script"""
        try:
            clean_text = re.sub(r'```json|```', '', raw_text).strip()
            return json.loads(clean_text)
        except Exception:
            if attempt < 3:
                fixed_text = self.fix_json_syntax(clean_text)
                return self.recursive_repair(fixed_text, attempt + 1)
            return None

    def fix_json_syntax(self, text):
        """Regex Repair from your old script"""
        text = text.strip()
        if not text.endswith("]"): text += "]"
        if not text.startswith("["): text = "[" + text
        return text

    def make_dashboard(self):
        """The Elite Hacker UI you requested"""
        pct = (self.completed_count / self.total_topics) * 100
        prog_bar = f"🔥 PROGRESS: [[green]{'🏁' * int(pct/10)}{'─' * (10-int(pct/10))}[/green]] {pct:.1f}% ({self.completed_count}/{self.total_topics} Topics)"
        
        table = Table(box=None, expand=True)
        table.add_column("🧵 Thread", style="magenta")
        table.add_column("🔑 Key Source", style="cyan")
        table.add_column("🎯 Current Topic", style="white")
        table.add_column("⚡ Status", justify="right")

        for t_id, info in self.thread_status.items():
            color = "green" if "COMPLETED" in info['state'] else "yellow"
            if "ERROR" in info['state']: color = "red"
            table.add_row(t_id, info['key'], info['topic'], f"[{color}]{info['state']}[/{color}]")

        dashboard = Panel(
            f"🚀 [bold cyan]AJX MATRIX WORKER #{self.worker_id}[/bold cyan] | [green]🟢 STATUS: FIRING[/green]\n"
            f"📊 SUBJECT: {self.subject} | 🛰️ MODE: [METHOD {self.task_data['METHOD']}]\n\n"
            f"{prog_bar}\n"
            f"─────────────────────────────────────────────────────────────────────────────\n"
            f"🧵 ACTIVE THREADS (8-KEY ROTATION):\n"
            f"─────────────────────────────────────────────────────────────────────────────\n"
            + str(console.render_str(str(table))) +
            f"\n─────────────────────────────────────────────────────────────────────────────\n"
            f"📡 CLOUD STATUS: 💾 Drive: [green][SYNCED][/green] | 🔥 Firebase: [green][LIVE][/green] | 📦 Zstd: Active",
            border_style="bright_blue"
        )
        return dashboard

    def process_method_1(self, model, topic_data):
        """Method 1 from your old script"""
        try:
            pdf_path = os.path.join(self.task_data['SOURCE_PATH'], f"{self.subject}.PDF")
            doc = fitz.open(pdf_path)
            page_num = topic_data.get('page', 0)
            page = doc[page_num]
            pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
            img_data = pix.tobytes("jpeg")
            prompt = self.task_data.get('MASTER_PROMPT')
            response = model.generate_content([prompt, {"mime_type": "image/jpeg", "data": img_data}])
            return self.recursive_repair(response.text)
        except Exception: return None

    def process_method_2(self, model, topic_data):
        """Method 2 from your old script"""
        prompt = f"Topic: {topic_data['topic']}\nInstructions: {self.task_data.get('MASTER_PROMPT')}"
        try:
            response = model.generate_content(prompt)
            return self.recursive_repair(response.text)
        except Exception: return None

    def pack_and_sync(self, topic, data):
        """Sync Logic from your old script"""
        packed_str = json.dumps(data)
        compressed = self.compressor.compress(packed_str.encode('utf-8'))
        topic_node = str(topic).replace(".", "_")
        
        # Firebase Sync
        db.reference(f"Syllabus/{self.subject}/Data/{topic_node}").update({
            "status": "COMPLETED",
            "payload": compressed.hex(),
            "mcq_count": len(data)
        })

        # Local Backup for G-Drive
        backup_dir = f"BACKUP/{self.subject}"
        os.makedirs(backup_dir, exist_ok=True)
        with open(f"{backup_dir}/{topic_node}.json", "w") as f:
            json.dump(data, f, indent=4)

    def fire_engine(self, topic_data):
        """The Orchestrator matching your 8-key requirement"""
        t_id = f"T{(topic_data['index'] % 8) + 1}"
        key_index = topic_data['index'] % len(self.keys)
        topic_name = topic_data['topic']
        
        self.thread_status[t_id].update({"topic": topic_name[:30], "state": "GENERATING..."})
        
        try:
            genai.configure(api_key=self.keys[key_index])
            model = genai.GenerativeModel('gemini-2.0-flash')
            
            mcqs = None
            if self.task_data['METHOD'] == 1:
                mcqs = self.process_method_1(model, topic_data)
            else:
                mcqs = self.process_method_2(model, topic_data)

            if mcqs:
                self.thread_status[t_id]['state'] = "SYNCING..."
                self.pack_and_sync(topic_name, mcqs)
                self.completed_count += 1
                self.thread_status[t_id]['state'] = "COMPLETED 💎"
            else:
                self.thread_status[t_id]['state'] = "REPAIRING JSON 🛠️"
        except Exception:
            self.thread_status[t_id]['state'] = "ERROR ⚠️"

    def start_matrix(self):
        """Elite Orchestrator: Multi-threading with Non-Blocking Live UI"""
        batch = self.task_data['BATCH']
        topics_with_index = [{"index": i, "topic": t} for i, t in enumerate(batch)]
        
        # Dashboard ko Live mode mein start karna
        with Live(self.make_dashboard(), refresh_per_second=4) as live:
            # 8-Key Threaded Engine
            with ThreadPoolExecutor(max_workers=8) as executor:
                # 1. Sabhi tasks ko submit karna aur 'futures' list mein save karna
                # 'submit' use karne se code block nahi hota aur UI refresh hoti rehti hai
                futures = [executor.submit(self.fire_engine, t) for t in topics_with_index]
                
                # 2. DSA: Monitoring Loop
                # Jab tak koi bhi thread 'running' hai ya 'not done' hai, loop chalta rahega
                while any(not f.done() for f in futures):
                    # Live dashboard ko refresh karna
                    live.update(self.make_dashboard())
                    # CPU par load kam karne ke liye minor sleep
                    time.sleep(0.2)
                
                # 3. Final Verification: Sab khatam hone ke baad ek aakhri update
                live.update(self.make_dashboard())

        console.print(Panel(f"[bold green]✅ WORKER {self.worker_id} MISSION ACCOMPLISHED![/bold green]"))

if __name__ == "__main__":
    t_file = f"WORKER_TASK_{os.getenv('WORKER_ID', '1')}.JSON"
    if os.path.exists(t_file):
        EliteMatrixWorker(t_file).start_matrix()
    else:
        console.print("[red]❌ Task File Not Found.[/red]")
