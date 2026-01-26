import os
import json
import sys

# --- 🔒 FIXED SETTINGS (Change here if needed) ---
FIXED_EXAM = "UPSI"
FIXED_SUBJECT = "History"
MIN_Q_PER_PAGE = 30
MAX_Q_PER_PAGE = 100

# --- PATHS ---
WORKFLOW_DB_FILE = 'workflows.json'
OUTPUT_ROOT_BASE = 'AJX_OUTPUT_Phase2'
PROMPT_FILE = 'master_prompt.txt'

def main():
    print(f"🤖 HEADLESS ARCHITECT STARTED")
    print(f"   Exam Fixed: {FIXED_EXAM}")
    print(f"   Subject Fixed: {FIXED_SUBJECT}")

    # 1. Read the Master Prompt
    if not os.path.exists(PROMPT_FILE):
        print(f"❌ Error: '{PROMPT_FILE}' nahi mila! Pehle step 2 complete karo.")
        sys.exit(1)

    with open(PROMPT_FILE, "r", encoding="utf-8") as f:
        prompt_content = f.read()

    # 2. Create Workflow Structure
    workflow_name = f"{FIXED_EXAM}_{FIXED_SUBJECT}"

    workflow_data = {
        "exam_name": FIXED_EXAM,
        "subject": FIXED_SUBJECT,
        "min_q": MIN_Q_PER_PAGE,
        "max_q": MAX_Q_PER_PAGE,
        "prompt_template": prompt_content,
        "output_folder": os.path.join(OUTPUT_ROOT_BASE, workflow_name)
    }

    # 3. Save to JSON (Overwrite if exists)
    final_db = {workflow_name: workflow_data}

    with open(WORKFLOW_DB_FILE, 'w', encoding='utf-8') as f:
        json.dump(final_db, f, indent=4)

    print(f"✅ Configuration Saved in '{WORKFLOW_DB_FILE}'")
    print(f"   Target Folder: {workflow_data['output_folder']}")

if __name__ == "__main__":
    main()
