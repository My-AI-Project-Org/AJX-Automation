import os
import json
import time
import sys
import math
import google.generativeai as genai
from PIL import Image

# 👇 NEW LIBRARY FOR COLORFUL UI
from rich.console import Console
from rich.progress import (
    Progress,
    SpinnerColumn,
    BarColumn,
    TextColumn,
    TimeElapsedColumn,
    TaskProgressColumn
)
from rich.panel import Panel
from rich.text import Text

# --- ARGUMENTS ---
if len(sys.argv) != 3:
    print("❌ Error: Worker ID missing.")
    sys.exit(1)

TOTAL_WORKERS = int(sys.argv[1])
WORKER_ID = int(sys.argv[2])

# --- SETUP ---
INPUT_ROOT = 'AJX_Phase1_Output'
WORKFLOW_DB_FILE = 'workflows.json'
console = Console()

# --- LOAD CONFIG ---
if not os.path.exists(WORKFLOW_DB_FILE):
    sys.exit("❌ Error: workflows.json missing.")

with open(WORKFLOW_DB_FILE, 'r') as f:
    db = json.load(f)
    wf_key = list(db.keys())[0]
    config = db[wf_key]

OUTPUT_DIR = config['output_folder']

# --- API KEY MANAGER ---
API_KEYS = json.loads(os.environ.get("GEMINI_API_KEYS_LIST", "[]"))
if not API_KEYS:
    sys.exit("❌ Error: No API Keys.")

curr_key_idx = (WORKER_ID - 1) % len(API_KEYS)

def get_model():
    genai.configure(api_key=API_KEYS[curr_key_idx])
    return genai.GenerativeModel('gemini-2.5-flash', generation_config={"response_mime_type": "application/json"})

model = get_model()

def rotate_key():
    global curr_key_idx, model
    console.print(f"[bold yellow]🔄 Limit Hit. Rotating API Key...[/bold yellow]")
    curr_key_idx = (curr_key_idx + 1) % len(API_KEYS)
    model = get_model()
    time.sleep(2)

# --- MAIN LOGIC ---
# Colorful Header
header_text = Text(f"WORKER {WORKER_ID}/{TOTAL_WORKERS} ONLINE", style="bold white on blue")
header_text.append(f"\nTarget Exam: {config['exam_name']}", style="bold cyan")
console.print(Panel(header_text, border_style="blue"))

# 1. Collect Tasks
all_tasks = []
for root, _, files in os.walk(INPUT_ROOT):
    files.sort()
    for file in files:
        if file.lower().endswith(('.jpg', '.png')):
            src_path = os.path.join(root, file)
            rel_path = os.path.relpath(src_path, INPUT_ROOT)
            dest_path = os.path.join(OUTPUT_DIR, os.path.dirname(rel_path), os.path.splitext(file)[0] + ".json")
            all_tasks.append({'src': src_path, 'dest': dest_path, 'id': rel_path})

# 2. Assign Work
all_tasks.sort(key=lambda x: x['id'])
chunk_size = math.ceil(len(all_tasks) / TOTAL_WORKERS)
start_idx = (WORKER_ID - 1) * chunk_size
end_idx = start_idx + chunk_size
my_tasks = all_tasks[start_idx:end_idx]

console.print(f"[bold green]📊 My Load:[/bold green] {len(my_tasks)} Images")

# 3. Processing with RICH PROGRESS BAR
# We create a layout with columns for a beautiful effect
with Progress(
    SpinnerColumn(spinner_name="dots12", style="bold magenta"), # Ghoomne wala chakkar
    TextColumn("[bold blue]{task.description}"), # Text Description
    BarColumn(bar_width=40, style="cyan", complete_style="bold green"), # Colorful Bar
    TaskProgressColumn(), # Percentage
    TimeElapsedColumn(),  # Time Taken
    console=console
) as progress:
    
    # Create the Main Task Bar
    main_task = progress.add_task(f"[bold cyan]Worker {WORKER_ID} Overall Progress", total=len(my_tasks))
    
    for task in my_tasks:
        file_name = os.path.basename(task['id'])
        
        if os.path.exists(task['dest']):
            progress.console.print(f"   [dim]⏭️  Skipped {file_name}[/dim]")
            progress.advance(main_task)
            continue

        os.makedirs(os.path.dirname(task['dest']), exist_ok=True)

        try:
            img = Image.open(task['src'])
            
            # --- STEP A: ANALYSIS (Spinner Mode) ---
            progress.update(main_task, description=f"[bold yellow]🔍 ANALYZING:[/bold yellow] {file_name}")
            
            try:
                res = model.generate_content(["Estimate MCQ count JSON: {'count': N}", img])
                estimated = int(json.loads(res.text).get('count', 30))
            except:
                estimated = 30
            
            target_count = max(config['min_q'], min(estimated, config['max_q']))
            
            # --- STEP B: GENERATION (Bar fills up) ---
            collected_data = []
            
            # Inner "Virtual" progress updates via description
            while len(collected_data) < target_count:
                needed = min(25, target_count - len(collected_data))
                
                # Update Text to show Generation Status (e.g., Generating 10/30)
                progress.update(main_task, description=f"[bold green]⚡ GENERATING:[/bold green] {file_name} ({len(collected_data)}/{target_count})")
                
                final_prompt = config['prompt_template'].format(
                    target_count=needed,
                    start_id=len(collected_data) + 1,
                    exam_name=config['exam_name']
                )

                try:
                    response = model.generate_content([final_prompt, img])
                    batch_mcqs = json.loads(response.text)
                    if batch_mcqs:
                        collected_data.extend(batch_mcqs)
                    else:
                        break
                except Exception as e:
                    if "429" in str(e):
                        rotate_key()
                    else:
                        break
            
            # Save
            if collected_data:
                with open(task['dest'], 'w', encoding='utf-8') as f:
                    json.dump(collected_data, f, indent=4)
        
        except Exception as e:
            progress.console.print(f"[bold red]❌ Error on {file_name}: {e}[/bold red]")

        # Move the Main Bar forward
        progress.advance(main_task)

console.print(f"[bold white on green] ✅ WORKER {WORKER_ID} FINISHED! [/bold white on green]")
