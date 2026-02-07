import os
import json
import time
import re
import math
import shutil
import fitz  # PyMuPDF
from natsort import natsorted
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress

# --- NEW IMPORT FOR AI ---
import google.generativeai as genai
console = Console()
# --- FIREBASE SETUP ---
import firebase_admin
from firebase_admin import credentials, db

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
        print(f"⚠️ Firebase Warning: {e} (Running in Offline Mode)")

# --- GEMINI SETUP (UPDATED) ---
# --- GEMINI SETUP (DIRECT DEDICATED KEY) ---
# Yahan apni 'AIza...' wali key paste kar dena quotes ke andar
DEDICATED_KEY =  "AIzaSyDmb1hHM0Qn_BKllH0Ev9xVU1EG8k6_53c"

try:
    genai.configure(api_key=DEDICATED_KEY)
    console.print(f"[green]✅ Gemini Configured with Dedicated Key ending in ...{DEDICATED_KEY[-4:]}[/green]")
except Exception as e:
    console.print(f"[red]❌ Gemini Configuration Error: {e}[/red]")


# --- CONFIGURATION ---
BASE_DIRS = ["METHOD_1", "METHOD_2"] 
ASSETS_DIR_NAME = "EXTRACTED_ASSETS"
TASK_FILE_PREFIX = "WORKER_TASK"

class AJXScoutElite:
    def __init__(self):
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
            # LOGGING
            console.print(f"[dim]📡 Syncing Status: {status} - {message}[/dim]")
        except:
            pass 

    def clean_filename(self, name):
        """Sanitizes folder names."""
        clean = re.sub(r'[^\w\s-]', '', name)
        clean = re.sub(r'[-\s]+', '_', clean).strip()
        return clean[:50] 

    # ==========================================
    # 🧠 NEW LOGIC: GEMINI FILE UPLOAD + DFS + LOOP
    # ==========================================

    def wait_for_file_active(self, file):
        """Waits for Google Server to process the PDF."""
        console.print(f"[yellow]⏳ Waiting for file processing: {file.name}...[/yellow]")
        while True:
            file = genai.get_file(file.name)
            if file.state.name == "ACTIVE":
                console.print(f"[green]✅ File Ready on Google Server![/green]")
                return file
            elif file.state.name == "FAILED":
                raise Exception("Google File Processing Failed.")
            time.sleep(2)

    def get_structure_from_gemini(self, index_pdf_path, subject):
        """
        REPLACES REGEX: Uploads PDF -> DFS Prompt -> Retry Loop -> Valid JSON
        """
        console.print(f"[bold cyan]🔍 STEP 1: Starting Gemini Index Analysis for {subject}...[/bold cyan]")
        self.update_remote_monitor(subject, "UPLOADING", 5, "Uploading Index to AI")

        try:
            # 1. Upload
            uploaded_file = genai.upload_file(path=index_pdf_path, display_name="Index PDF")
            active_file = self.wait_for_file_active(uploaded_file)

            # 2. Architect Prompt (DFS & Nested Logic)
            prompt = """
            Act as a Syllabus Architect. Parse the uploaded Table of Contents PDF.
            
            **CRITICAL RULES:**
            1. **DFS Hierarchy:** Create a NESTED structure. Unit -> Chapters -> Topics (if indented).
            2. **Unit Detection:** Detect logical Units (e.g., 'Ancient History') based on bold headers or numbering resets.
            3. **Clean Numbers:** Convert 'B455' to 455. Remove prefixes.
            4. **Completeness:** Do not stop until the end of the document.

            **OUTPUT FORMAT (Strict JSON):**
            [
              {
                "unit_name": "Unit Name",
                "chapters": [
                  { "chapter": "Chapter Name", "start_p": 10, "end_p": 20 }
                ]
              }
            ]
            """

            # 3. Multipass Loop
            model = genai.GenerativeModel("gemini-2.5-flash")
            attempts = 0
            
            while attempts < 3:
                console.print(f"[yellow]🤖 Gemini Thinking... (Attempt {attempts+1}/3)[/yellow]")
                self.update_remote_monitor(subject, "ANALYZING", 10, f"AI Attempt {attempts+1}")
                
                try:
                    response = model.generate_content([active_file, prompt])
                    
                    # Clean JSON
                    raw_text = response.text.replace("```json", "").replace("```", "").strip()
                    structure = json.loads(raw_text)

                    # Validation
                    if isinstance(structure, list) and len(structure) > 0:
                        if 'chapters' in structure[0]:
                            console.print(f"[green]✅ Valid Nested Skeleton Found: {len(structure)} Units[/green]")
                            return structure
                        else:
                            console.print("[red]⚠️ JSON missing 'chapters'. Retrying...[/red]")
                    else:
                        console.print("[red]⚠️ Empty JSON. Retrying...[/red]")

                except Exception as inner_e:
                    console.print(f"[red]⚠️ Error in Attempt {attempts+1}: {inner_e}[/red]")
                
                attempts += 1
                time.sleep(2)
            
            console.print("[bold red]❌ All Attempts Failed. Could not parse Index.[/bold red]")
            return None

        except Exception as e:
            console.print(f"[red]❌ Critical Gemini Error: {e}[/red]")
            return None

    def calculate_5_level_offset(self, main_pdf_path, structure):
        """ALGORITHM: Finds difference between Index Page and Real PDF Page."""
        try:
            # Anchor from first chapter
            anchor_chap = structure[0]['chapters'][0]
            target_name = anchor_chap['chapter'].lower()[:15]
            target_index_page = anchor_chap['start_p']
            
            console.print(f"[yellow]⚓ Calculating Offset... Searching for '{target_name}' (Index says Page {target_index_page})[/yellow]")
            
            doc = fitz.open(main_pdf_path)
            detected_page = -1
            
            for i in range(min(50, len(doc))):
                page = doc[i]
                if target_name in page.get_text().lower():
                    detected_page = i + 1
                    console.print(f"[green]✅ Anchor Found at PDF Page {detected_page}[/green]")
                    break
            
            if detected_page != -1:
                return detected_page - target_index_page
            return 0
        except:
            console.print("[red]⚠️ Offset Calculation Failed. Defaulting to 0.[/red]")
            return 0

    def extract_and_persist_smart(self, pdf_path, structure, offset, output_root, subject_name):
        """
        SMART ENGINE: Creates Tasks & Extracts Images using SEQUENTIAL COUNTING (Clean PDF Mode)
        """
        doc = fitz.open(pdf_path)
        master_tasks = []
        
        total_chapters = sum(len(u['chapters']) for u in structure)
        completed_chapters = 0
        
        # 👇👇👇 NEW LOGIC: Start at Page 1 of Clean PDF 👇👇👇
        current_cursor = 1 
        
        with Progress() as progress:
            task_bar = progress.add_task("[cyan]🏭 Manufacturing Assets...", total=total_chapters)
            
            unit_counter = 1
            for unit in structure:
                safe_unit = f"{unit_counter:02d}_{self.clean_filename(unit['unit_name'])}"
                unit_path = os.path.join(output_root, safe_unit)
                os.makedirs(unit_path, exist_ok=True)
                
                chap_counter = 1
                for chap in unit['chapters']:
                    safe_chap = f"{chap_counter:02d}_{self.clean_filename(chap['chapter'])}"
                    chap_full_path = os.path.join(unit_path, safe_chap)
                    os.makedirs(chap_full_path, exist_ok=True)
                    
                    # 👇👇👇 CRITICAL MATH CHANGE 👇👇👇
                    # Index se sirf LENGTH (Count) nikalo
                    page_count = (chap['end_p'] - chap['start_p']) + 1
                    
                    # Clean PDF se utna hissa kaat lo (Sequence mein)
                    real_start = current_cursor
                    real_end = current_cursor + page_count - 1
                    
                    # Cursor ko aage badhao agle chapter ke liye
                    current_cursor = real_end + 1
                    # 👆👆👆 LOGIC END 👆👆👆
                    relative_path = os.path.join(ASSETS_DIR_NAME, safe_unit, safe_chap)
                    
                    image_list = []
                    
                    # --- SMART RESUME CHECK ---
                    expected_count = (real_end - real_start) + 1
                    existing_files = [f for f in os.listdir(chap_full_path) if f.endswith('.jpg')]
                    
                    if len(existing_files) >= expected_count and expected_count > 0:
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
        console.print(Panel("[bold blue]🤖 AJX SCOUT: FINAL REPAIRED VERSION (GEMINI + SYNC)[/bold blue]"))
        
        for method_dir in BASE_DIRS:
            if not os.path.exists(method_dir): continue
            
            subjects = natsorted([d for d in os.listdir(method_dir) if os.path.isdir(os.path.join(method_dir, d))])
            
            for subject in subjects:
                console.print(f"\n[bold magenta]🚀 Processing: {subject} ({method_dir})[/bold magenta]")
                subject_path = os.path.join(method_dir, subject)
                self.update_remote_monitor(subject, "STARTED", 0, "Initializing")
                
                # SDUI Logic (Included)
                ui_file_path = os.path.join(subject_path, "ui_config.json")
                if os.path.exists(ui_file_path):
                    try:
                        with open(ui_file_path, 'r', encoding='utf-8') as f:
                            ui_data = json.load(f)
                        db.reference(f'Syllabus/{subject}/Config/UI').set(ui_data)
                        console.print("[green]✅ UI Config Uploaded to Firebase[/green]")
                    except Exception as e:
                        console.print(f"[red]⚠️ UI Config Error: {e}[/red]")

                master_tasks = []

                # --- METHOD 1: GEMINI PDF MODE ---
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
                    
                    # 1. PARSE STRUCTURE (AI + WAIT LOOP)
                    structure = self.get_structure_from_gemini(index_file, subject)
                    if not structure: 
                        console.print("[red]❌ Skipping Subject due to Index Error.[/red]")
                        continue
                    
                    # 🔥 CRITICAL LOGIC: SYNC SKELETON TO FIREBASE
                    console.print(f"[bold green]☁️ Syncing Skeleton to Firebase: Syllabus/{subject}/Structure...[/bold green]")
                    db.reference(f'Syllabus/{subject}/Structure').set(structure)
                    self.update_remote_monitor(subject, "SYNCED", 10, "Skeleton Uploaded")

                    console.print("[bold yellow]🚀 SEQUENTIAL MODE: Assuming Clean PDF (Chapter 1 = Page 1)[/bold yellow]")
                    self.update_remote_monitor(subject, "EXTRACTING", 15, "Assets Manufacturing")
                    
                    # Hum '0' pass kar rahe hain offset mein kyunki ab function khud Counting karega
                    master_tasks = self.extract_and_persist_smart(pdf_file, structure, 0, assets_root, subject)

                # --- METHOD 2: JSON MODE ---
                elif method_dir == "METHOD_2":
                    json_path = os.path.join(subject_path, "syllabus.json")
                    if not os.path.exists(json_path):
                        console.print(f"[red]❌ syllabus.json missing[/red]")
                        continue

                    self.update_remote_monitor(subject, "READING_JSON", 20, "Parsing Syllabus")
                    with open(json_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)

                    # 🔥 SYNC SKELETON
                    db.reference(f'Syllabus/{subject}/Structure').set(data)
                    console.print(f"[green]☁️ Skeleton Synced to Firebase[/green]")

                    assets_root = os.path.join(subject_path, ASSETS_DIR_NAME)
                    unit_counter = 1
                    global_id_counter = 101 
                    
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
                                "images": [],
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
                    console.print(f"[green]✅ {method_dir}: Tasks Generated & Saved![/green]")

if __name__ == "__main__":
    AJXScoutElite().execute()
