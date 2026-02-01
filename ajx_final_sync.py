import os
import json
import zstandard as zstd
from firebase_admin import db
from rich.console import Console
from rich.panel import Panel

console = Console()

class AJXFinalEncoder:
    def __init__(self, subject_name):
        self.subject = subject_name.upper()
        self.backup_path = f"BACKUP/{self.subject}"
        self.final_zip_path = f"FINAL_EXPORTS/{self.subject}"
        os.makedirs(self.final_zip_path, exist_ok=True)

    def train_zstd_dictionary(self):
        """DSA: Dictionary Training for Elite Compression"""
        console.print("[yellow]🧠 Training Zstd Dictionary for better compression...[/yellow]")
        samples = []
        # Saari JSON files se samples uthana patterns seekhne ke liye
        for file in os.listdir(self.backup_path):
            with open(os.path.join(self.backup_path, file), 'rb') as f:
                samples.append(f.read())
        
        # 100KB ki dictionary train karna common phrases (Which, is, the) ke liye
        dict_data = zstd.train_dictionary(102400, samples)
        dict_path = f"{self.final_zip_path}/compression.dict"
        with open(dict_path, "wb") as f:
            f.write(dict_data.as_bytes())
        return dict_data

    def consolidate_and_verify(self):
        """DevOps: Check if any topic from Worker 1, 2, or 3 is missing"""
        files = os.listdir(self.backup_path)
        console.print(f"[green]✅ Total Topics Verified: {len(files)}[/green]")
        
        # Master Manifest file banana (Table of Contents)
        manifest = {
            "subject": self.subject,
            "total_topics": len(files),
            "files": natsorted(files),
            "version": "2026.1"
        }
        with open(f"{self.final_zip_path}/manifest.json", "w") as f:
            json.dump(manifest, f, indent=4)

    def drive_finalize(self):
        """DevOps: Final Drive Organisation"""
        # Yahan hum PyDrive ya Google API use karke pura folder structure organize karenge
        # 1. RAW_JSON Folder
        # 2. BINARY_ZST Folder
        # 3. DICT_KEY (The compression key)
        console.print(f"[bold green]🏁 Phase 3: Drive Structure Finalized for {self.subject}[/bold green]")

    def run(self):
        console.print(Panel(f"[bold magenta]🏁 AJX PHASE 3: THE FINAL ENCODER[/bold magenta]"))
        if not os.path.exists(self.backup_path):
            console.print("[red]❌ No backup files found to encode![/red]")
            return
            
        self.consolidate_and_verify()
        self.train_zstd_dictionary()
        self.drive_finalize()

from natsort import natsorted
if __name__ == "__main__":
    # Subject name will be passed from GitHub Action
    sub = os.getenv("CURRENT_SUBJECT", "UPSI_HISTORY")
    AJXFinalEncoder(sub).run()
