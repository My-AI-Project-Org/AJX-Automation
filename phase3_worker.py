import os
import json
import time
import sys
import math
import google.generativeai as genai
from PIL import Image

# --- ARGUMENTS (GitHub Actions se aayenge) ---
# Command: python phase3_worker.py <TOTAL_WORKERS> <WORKER_ID>
if len(sys.argv) != 3:
    print("❌ Error: Worker ID missing.")
    sys.exit(1)

TOTAL_WORKERS = int(sys.argv[1])
WORKER_ID = int(sys.argv[2])

# --- SETUP PATHS ---
INPUT_ROOT = 'AJX_Phase1_Output'
WORKFLOW_DB_FILE = 'workflows.json'

# --- LOAD CONFIG ---
if not os.path.exists(WORKFLOW_DB_FILE):
    sys.exit("❌ Error: workflows.json missing. Run Phase 2 first.")

with open(WORKFLOW_DB_FILE, 'r') as f:
    db = json.load(f)
    # Automatically pick the first (and only) workflow
    wf_key = list(db.keys())[0]
    config = db[wf_key]

OUTPUT_DIR = config['output_folder']

# --- API KEY MANAGER ---
API_KEYS = json.loads(os.environ.get("GEMINI_API_KEYS_LIST", "[]"))
if not API_KEYS:
    sys.exit("❌ Error: No API Keys found in Secrets.")

# Smart Offset: Worker 1 uses Key 1, Worker 2 uses Key 2...
curr_key_idx = (WORKER_ID - 1) % len(API_KEYS)

def get_model():
    genai.configure(api_key=API_KEYS[curr_key_idx])
    return genai.GenerativeModel('gemini-1.5-flash', generation_config={"response_mime_type": "application/json"})

model = get_model()

def rotate_key():
    global curr_key_idx, model
    print(f"🔄 Limit Hit. Rotating to next API Key...")
    curr_key_idx = (curr_key_idx + 1) % len(API_KEYS)
    model = get_model()
    time.sleep(2)

# --- MAIN LOGIC ---
print(f"👷 WORKER {WORKER_ID} STARTED (Exam: {config['exam_name']})")

# 1. Collect All Images
all_tasks = []
for root, _, files in os.walk(INPUT_ROOT):
    files.sort() # Sorting is critical for sync
    for file in files:
        if file.lower().endswith(('.jpg', '.png')):
            src_path = os.path.join(root, file)
            rel_path = os.path.relpath(src_path, INPUT_ROOT)
            # Output Path: AJX_OUTPUT/UPSI_History/Unit1/1.json
            dest_path = os.path.join(OUTPUT_DIR, os.path.dirname(rel_path), os.path.splitext(file)[0] + ".json")
            all_tasks.append({'src': src_path, 'dest': dest_path, 'id': rel_path})

# 2. Distribute Work
all_tasks.sort(key=lambda x: x['id'])
chunk_size = math.ceil(len(all_tasks) / TOTAL_WORKERS)
start_idx = (WORKER_ID - 1) * chunk_size
end_idx = start_idx + chunk_size
my_tasks = all_tasks[start_idx:end_idx]

print(f"📊 My Task Count: {len(my_tasks)} images")

# 3. Processing Loop
for task in my_tasks:
    if os.path.exists(task['dest']):
        continue # Skip if already done

    # Create folder
    os.makedirs(os.path.dirname(task['dest']), exist_ok=True)

    try:
        img = Image.open(task['src'])

        # Step A: Estimate Count
        try:
            prompt_analysis = "Analyze this page. Estimate how many high-quality MCQs can be extracted. Return JSON: {'count': N}"
            res = model.generate_content([prompt_analysis, img])
            estimated = int(json.loads(res.text).get('count', 30))
        except:
            estimated = 30

        target_count = max(config['min_q'], min(estimated, config['max_q']))

        # Step B: Generate in Batches (25 at a time)
        collected_data = []

        while len(collected_data) < target_count:
            needed = min(25, target_count - len(collected_data))

            # --- ID LOGIC ---
            # Pass 1 (len=0) -> start_id = 1
            # Pass 2 (len=25) -> start_id = 26
            current_start_id = len(collected_data) + 1

            # Fill Prompt
            final_prompt = config['prompt_template'].format(
                target_count=needed,
                start_id=current_start_id,
                exam_name=config['exam_name'] # Just in case placeholder exists
            )

            try:
                response = model.generate_content([final_prompt, img])
                batch_mcqs = json.loads(response.text)

                if batch_mcqs:
                    collected_data.extend(batch_mcqs)
                else:
                    break # Stop if AI returns empty

            except Exception as e:
                if "429" in str(e):
                    rotate_key()
                else:
                    print(f"⚠️ Warning on {task['id']}: {e}")
                    break

        # Save Result
        if collected_data:
            with open(task['dest'], 'w', encoding='utf-8') as f:
                json.dump(collected_data, f, indent=4)

    except Exception as e:
        print(f"❌ Critical Error on {task['id']}: {e}")

print(f"✅ WORKER {WORKER_ID} FINISHED!")
