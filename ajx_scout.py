import os
import json
import fitz  # PyMuPDF
import re
import shutil
import math
import time
from natsort import natsorted
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress

# --- FIREBASE SETUP ---
import firebase_admin
from firebase_admin import credentials, db

# Initialize Firebase
if not firebase_admin._apps:
    try:
        key_json = os.environ.get("FIREBASE_SERVICE_KEY")
        if key_json:
            cred = credentials.Certificate(json.loads(key_json))
        else:
            # Fallback for local testing
            cred = credentials.Certificate("serviceAccountKey.json") 
            
        firebase_admin.initialize_app(cred, {
            'databaseURL': os.environ.get("FIREBASE_DB_URL")
        })
    except Exception as e:
        print(f"⚠️ Firebase Warning: {e} (Running in Offline Mode)")

console = Console()

# --- CONFIGURATION ---
BASE_DIRS = ["METHOD_1", "METHOD_2"] 
ASSETS_DIR_NAME = "EXTRACTED_ASSETS"
TASK_FILE_PREFIX = "WORKER_TASK"

class AJXScoutElite:
    def __init__(self):
        # Dynamic keywords for Unit detection
        self.unit_keywords = [
            "ANCIENT HISTORY", "MEDIEVAL HISTORY", "MODERN HISTORY",
            "GEOGRAPHY", "POLITY", "ECONOMY", "SCIENCE", "ENVIRONMENT",
            "SECTION", "UNIT", "PART", "KHAND", "GENERAL STUDIES"
        ]
        self.monitor_ref = None

    def update_remote_monitor(self, subject, status, progress, message):
        """FIREBASE BRAIN: Real-time status update."""
        try:
            if not self.monitor_ref:
                self.monitor_ref = db.reference(f'Monitoring/{subject}/Scout')
            
            self.monitor_ref.update({
                "status": status,
                "progress": progress,
                "current_task": message,
                "last_updated": int(time.time() * 1000)
            })
        except:
            pass 

    def clean_filename(self, name):
        """Sanitizes folder names."""
        clean = re.sub(r'[^\w\s-]', '', name)
        clean = re.sub(r'[-\s]+', '_', clean).strip()
        return clean[:50] 

    # ==========================================
    # 🧠 METHOD 1 LOGIC (PDF PARSING)
    # ==========================================

    def parse_index_structure(self, index_pdf_path):
        """DSA: Parsing Index PDF into a Structured N-ary Tree."""
        console.print(f"[cyan]🔍 Analyzing Index Structure...[/cyan]")
        doc = fitz.open(index_pdf_path)
        full_text = ""
        for page in doc: full_text += page.get_text() + "\n"
        
        lines = full_text.split('\n')
        chapter_pattern = re.compile(r'(\d+)\.\s+(.*?)\s+([B]?\d+\s*-\s*[B]?\d+)')
        
        detected_items = []
        current_unit = "General_Section"
        
        for line in lines:
            line = line.strip()
            if not line: continue
            
            # Unit Detection
            line_upper = line.upper()
            for kw in self.unit_keywords:
                if kw in line_upper and len(line) < 60:
                    current_unit = self.clean_filename(line)
                    break
            
            # Chapter Detection
            match = chapter_pattern.search(line)
            if match:
                try:
                    topic_name = match.group(2).strip()
                    range_str = match.group(3).strip().replace(" ", "")
                    numbers = re.findall(r'\d+', range_str)
                    if len(numbers) >= 2:
                        detected_items.append({
                            "unit": current_unit,
                            "chapter": self.clean_filename(topic_name),
                            "original_topic": topic_name,
                            "start_p": int(numbers[0]),
                            "end_p": int(numbers[1])
                        })
                except: continue

        # Grouping
        detected_items.sort(key=lambda x: x['unit']) 
        structure = []
        from itertools import groupby
        for key, group in groupby(detected_items, key=lambda x: x['unit']):
            structure.append({"unit_name": key, "chapters": list(group)})
            
        return structure

    def calculate_5_level_offset(self, main_pdf_path, anchor_chapter):
        """ALGORITHM: 5-Level Anchoring to find Real Offset."""
        console.print("[yellow]⚓ Calculating Offset...[/yellow]")
        doc = fitz.open(main_pdf_path)
        target_name = anchor_chapter['original_topic'].lower()[:15]
        target_index_page = anchor_chapter['start_p']
        detected_page = -1
        
        for i in range(min(30, len(doc))):
            page = doc[i]
            blocks = page.get_text("blocks")
            plain_text = page.get_text().lower()
            
            if target_name in plain_text:
                if len(blocks) > 5: # Content Density Check
                    detected_page = i + 1
                    console.print(f"[green]✅ Anchor Found at PDF Page {detected_page}[/green]")
                    break
        
        return (detected_page - target_index_page) if detected_page != -1 else 0

    def extract_and_persist_smart(self, pdf_path, structure, offset, output_root, subject_name):
        """
        SMART ENGINE: Extracts Images & Resumes if stopped.
        """
        doc = fitz.open(pdf_path)
        master_tasks = []
        
        total_chapters = sum(len(u['chapters']) for u in structure)
        completed_chapters = 0
        
        with Progress() as progress:
            task_bar = progress.add_task("[cyan]🏭 Manufacturing Assets...", total=total_chapters)
            
            unit_counter = 1
            for unit in structure:
                safe_unit = f"{unit_counter:02d}_{unit['unit_name']}"
                unit_path = os.path.join(output_root, safe_unit)
                os.makedirs(unit_path, exist_ok=True)
                
                chap_counter = 1
                for chap in unit['chapters']:
                    safe_chap = f"{chap_counter:02d}_{chap['chapter']}"
                    chap_full_path = os.path.join(unit_path, safe_chap)
                    os.makedirs(chap_full_path, exist_ok=True)
                    
                    # Math for ID
                    real_start = chap['start_p'] + offset
                    real_end = chap['end_p'] + offset
                    relative_path = os.path.join(ASSETS_DIR_NAME, safe_unit, safe_chap)
                    
                    image_list = []
                    
                    # --- SMART RESUME CHECK ---
                    expected_count = (real_end - real_start) + 1
                    existing_files = [f for f in os.listdir(chap_full_path) if f.endswith('.jpg')]
                    
                    if len(existing_files) >= expected_count and expected_count > 0:
                        # SKIP EXTRACTION (Already Done)
                        image_list = [f"{i}.jpg" for i in range(1, expected_count + 1)]
                    else:
                        # DO EXTRACTION
                        local_img_id = 1
                        for p_idx in range(real_start - 1, real_end):
                            if p_idx >= len(doc): break
                            
                            page = doc[p_idx]
                            pix = page.get_pixmap(matrix=fitz.Matrix(2, 2)) # High Quality
                            
                            img_filename = f"{local_img_id}.jpg"
                            pix.save(os.path.join(chap_full_path, img_filename))
                            
                            image_list.append(img_filename)
                            local_img_id += 1
                    
                    # Task Entry
                    master_tasks.append({
                        "unit_name": unit['unit_name'],
                        "chapter_name": chap['chapter'],
                        "folder_path": relative_path,
                        "images": image_list,
                        "global_id_start": (real_start * 100) + 1,
                        "display_num_start": 1,
                        "mode": "IMAGE_MODE"
                    })
                    
                    chap_counter += 1
                    completed_chapters += 1
                    progress.advance(task_bar)
                    
                    if completed_chapters % 5 == 0:
                        pct = int((completed_chapters / total_chapters) * 100)
                        self.update_remote_monitor(subject_name, "EXTRACTING", pct, f"Processing {chap['chapter']}")
                
                unit_counter += 1
        
        return master_tasks

    # ==========================================
    # 🚀 MAIN EXECUTION
    # ==========================================

    def execute(self):
        console.print(Panel("[bold blue]🤖 AJX SCOUT: ELITE PRODUCER (DUAL MODE + SDUI)[/bold blue]"))
        
        for method_dir in BASE_DIRS:
            if not os.path.exists(method_dir): continue
            
            subjects = natsorted([d for d in os.listdir(method_dir) if os.path.isdir(os.path.join(method_dir, d))])
            
            for subject in subjects:
                console.print(f"\n[bold magenta]🚀 Processing: {subject} ({method_dir})[/bold magenta]")
                subject_path = os.path.join(method_dir, subject)
                self.update_remote_monitor(subject, "STARTED", 0, "Initializing")
                
                # ==========================================
                # 🟢 NEW: SERVER DRIVEN UI (SDUI) LOGIC
                # ==========================================
                ui_file_path = os.path.join(subject_path, "ui_config.json")
                if os.path.exists(ui_file_path):
                    console.print("[cyan]🎨 Found Server Driven UI Config! Syncing...[/cyan]")
                    try:
                        with open(ui_file_path, 'r', encoding='utf-8') as f:
                            ui_data = json.load(f)
                        
                        # Direct Upload to Firebase Config Node
                        db.reference(f'Syllabus/{subject}/Config/UI').set(ui_data)
                        console.print("[green]✅ UI Config Uploaded to Firebase[/green]")
                    except Exception as e:
                        console.print(f"[red]⚠️ UI Config Error: {e}[/red]")
                else:
                    console.print("[dim]No ui_config.json found. Using App Defaults.[/dim]")

                # --- CONTINUE WITH SCOUTING ---
                master_tasks = []

                # --- METHOD 1: PDF MODE ---
                if method_dir == "METHOD_1":
                    # Auto-Detect Files
                    pdf_file = None
                    index_file = None
                    for f in os.listdir(subject_path):
                        if f.lower().endswith(".pdf"):
                            if "index" in f.lower(): index_file = os.path.join(subject_path, f)
                            else: pdf_file = os.path.join(subject_path, f)
                    
                    if not pdf_file or not index_file:
                        console.print(f"[red]❌ Files missing in {subject}[/red]")
                        continue
                    
                    assets_root = os.path.join(subject_path, ASSETS_DIR_NAME)
                    
                    # Parse & Process
                    structure = self.parse_index_structure(index_file)
                    if not structure: continue
                    
                    first_unit = structure[0]['chapters']
                    offset = self.calculate_5_level_offset(pdf_file, first_unit[0]) if first_unit else 0
                    
                    self.update_remote_monitor(subject, "EXTRACTING", 10, "Smart Extraction Started")
                    master_tasks = self.extract_and_persist_smart(pdf_file, structure, offset, assets_root, subject)

                # --- METHOD 2: JSON MODE ---
                elif method_dir == "METHOD_2":
                    json_path = os.path.join(subject_path, "syllabus.json")
                    if not os.path.exists(json_path):
                        console.print(f"[red]❌ syllabus.json missing in {subject}[/red]")
                        continue

                    self.update_remote_monitor(subject, "READING_JSON", 20, "Parsing Syllabus")
                    with open(json_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)

                    assets_root = os.path.join(subject_path, ASSETS_DIR_NAME)
                    unit_counter = 1
                    global_id_counter = 101 # Start ID for JSON Mode
                    
                    for unit in data:
                        safe_unit = f"{unit_counter:02d}_{self.clean_filename(unit['unit_name'])}"
                        unit_path = os.path.join(assets_root, safe_unit)
                        os.makedirs(unit_path, exist_ok=True)
                        
                        chap_counter = 1
                        for chap_name in unit['chapters']:
                            safe_chap = f"{chap_counter:02d}_{self.clean_filename(chap_name)}"
                            chap_full_path = os.path.join(unit_path, safe_chap)
                            os.makedirs(chap_full_path, exist_ok=True)
                            
                            master_tasks.append({
                                "unit_name": unit['unit_name'],
                                "chapter_name": chap_name,
                                "folder_path": os.path.join(ASSETS_DIR_NAME, safe_unit, safe_chap),
                                "images": [], # Empty means Text Mode
                                "global_id_start": global_id_counter,
                                "display_num_start": 1,
                                "mode": "TEXT_ONLY"
                            })
                            global_id_counter += 100
                            chap_counter += 1
                        unit_counter += 1
                
                # --- COMMON: SPLIT & SAVE TASKS ---
                if master_tasks:
                    chunk_size = math.ceil(len(master_tasks) / 3)
                    for i in range(3):
                        batch = master_tasks[i * chunk_size : (i + 1) * chunk_size]
                        if not batch: continue
                        
                        prompt_txt = "Generate MCQs..."
                        prompt_path = os.path.join(subject_path, "MASTER_PROMPT.txt")
                        if os.path.exists(prompt_path):
                            with open(prompt_path, 'r') as f: prompt_txt = f.read()
                        
                        final_json = {
                            "SUBJECT": subject,
                            "SOURCE_ROOT": subject_path,
                            "BATCH_DATA": batch,
                            "MASTER_PROMPT": prompt_txt,
                            "WORKER_ID": i + 1,
                            "METHOD_TYPE": method_dir
                        }
                        
                        with open(f"{TASK_FILE_PREFIX}_{i+1}.json", 'w', encoding='utf-8') as f:
                            json.dump(final_json, f, indent=4)
                    
                    self.update_remote_monitor(subject, "SCOUT_COMPLETE", 100, "Tasks Ready")
                    console.print(f"[green]✅ {method_dir}: Tasks Generated Successfully![/green]")

if __name__ == "__main__":
    AJXScoutElite().execute()
