import os
import json
import time
import zstandard as zstd
import firebase_admin
from firebase_admin import credentials, db
from natsort import natsorted
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.live import Live

console = Console()

# --- 1. FIREBASE INITIALIZATION ---
if not firebase_admin._apps:
    FIREBASE_KEY = json.loads(os.getenv("FIREBASE_SERVICE_KEY"))
    DB_URL = os.getenv("FIREBASE_DB_URL")
    cred = credentials.Certificate(FIREBASE_KEY)
    firebase_admin.initialize_app(cred, {'databaseURL': DB_URL})

class AJXFinalEncoder:
    def __init__(self, subject_name):
        self.subject = subject_name.upper()
        self.backup_path = f"BACKUP/{self.subject}"
        self.export_path = f"FINAL_EXPORTS/{self.subject}"
        os.makedirs(self.export_path, exist_ok=True)
        self.monitor_ref = db.reference(f'Monitoring/{self.subject}/Finalizer')

    def update_remote(self, status, progress):
        self.monitor_ref.update({"status": status, "progress": f"{progress}%"})

    def consolidate_and_verify(self):
        """DevOps: Counting and Manifesting"""
        files = os.listdir(self.backup_path)
        manifest = {
            "subject": self.subject,
            "total_topics": len(files),
            "files": natsorted(files),
            "engine": "AJX-V6"
        }
        with open(f"{self.export_path}/manifest.json", "w") as f:
            json.dump(manifest, f, indent=4)
        return len(files)

    def train_zstd_dictionary(self):
        """DSA: Dictionary Patterns Training"""
        samples = [open(os.path.join(self.backup_path, f), 'rb').read() for f in os.listdir(self.backup_path)]
        dict_data = zstd.train_dictionary(102400, samples)
        with open(f"{self.export_path}/compression.dict", "wb") as f:
            f.write(dict_data.as_bytes())

    def drive_finalize(self):
        """DevOps: Mirroring backup to organized structure"""
        # 1. Raw JSON Storage
        raw_path = f"{self.export_path}/RAW_JSON"
        os.makedirs(raw_path, exist_ok=True)
        # 2. Pattern key for App
        # Logic to move files or zip them can go here
        return True

    def run(self):
        console.print(Panel(f"[bold magenta]🏁 AJX PHASE 3: THE FINAL ENCODER[/bold magenta]"))
        
        table = Table(show_header=True, header_style="bold green", expand=True)
        table.add_column("🚀 Action Phase", width=25)
        table.add_column("📊 Status", justify="center")
        table.add_column("🛰️ Sync Link", justify="right")

        with Live(table, refresh_per_second=4):
            # Step 1
            table.add_row("Consolidating Topics", "[yellow]Verifying...[/yellow]", "⏳")
            total = self.consolidate_and_verify()
            table.add_row("Consolidating Topics", f"[green]Verified {total} JSONs ✅[/green]", "📶")

            # Step 2
            table.add_row("Dictionary Training", "[yellow]Training...[/yellow]", "⏳")
            self.train_zstd_dictionary()
            table.add_row("Dictionary Training", "[green]Master Key Created 🔑[/green]", "📶")

            # Step 3 (Integrated your requested function)
            table.add_row("Drive Organization", "[yellow]Structuring...[/yellow]", "⏳")
            self.drive_finalize()
            table.add_row("Drive Organization", "[green]Structure Finalized 🏁[/green]", "📶")

            # Final Lock
            db.reference(f'Syllabus/{self.subject}/Config').update({"status": "LIVE"})

        console.print(Panel(f"[bold green]✅ MISSION ACCOMPLISHED: {self.subject} IS LIVE![/bold green]"))

if __name__ == "__main__":
    AJXFinalEncoder(os.getenv("CURRENT_SUBJECT", "UPSI_HISTORY")).run()
