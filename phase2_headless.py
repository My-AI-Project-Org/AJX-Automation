print("╔════════════════════════════════════════════════════╗")
print("║   AJX PHASE 2: BRAIN CONFIG (LIVE TERMINAL MODE)   ║")
print("╚════════════════════════════════════════════════════╝")

import os
import json
import time
import sys
import urllib.request
import urllib.parse
from collections import deque # 👈 Ye scrolling logs ke liye hai

# --- CONFIG ---
OUTPUT_FOLDER_NAME = 'AJX_Phase1_Output'
CONFIG_FILE_NAME = 'config.json'
PROMPT_FILE_NAME = 'master_prompt.txt'

# --- 🟢 LIVE TERMINAL SYSTEM (EXACT GITHUB COPY) ---
class TelegramTerminal:
    def __init__(self):
        self.token = os.environ.get("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.environ.get("TELEGRAM_CHAT_ID")
        self.message_id = None
        self.last_update_time = 0
        self.log_buffer = deque(maxlen=10) # Sirf last 10 lines dikhayega (Clean look)
        self.current_progress = 0
        self.current_status = "Initializing..."

    def start(self):
        if not self.token: return
        text = "<b>💻 AJX PHASE 2 CONSOLE</b>\nInitializing Brain..."
        self.message_id = self._send_new(text)

    def log_stream(self, msg):
        # Log add karo aur purana hatao (Scrolling effect)
        clean_msg = str(msg).replace("<", "&lt;").replace(">", "&gt;") 
        self.log_buffer.append(f"> {clean_msg}")
        self._refresh_display()

    def update_progress(self, percent, status):
        self.current_progress = percent
        self.current_status = status
        self._refresh_display()

    def _refresh_display(self):
        # Update throttle (1.5 sec gap taaki Telegram block na kare)
        if time.time() - self.last_update_time < 1.5 and self.current_progress < 100:
            return

        if not self.token or not self.message_id: return

        # Terminal View Build Karo
        logs_text = "\n".join(self.log_buffer)
        
        # ASCII Progress Bar
        bar_len = 10
        filled = int(bar_len * self.current_progress / 100)
        bar = "█" * filled + "░" * (bar_len - filled)

        # Final Message (GitHub Style)
        text = (
            f"<b>💻 AJX PHASE 2 CONSOLE</b>\n"
            f"<code>{logs_text}</code>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"<b>{self.current_status}</b>\n"
            f"<code>[{bar}] {self.current_progress}%</code>"
        )
        
        self._edit_msg(text)
        self.last_update_time = time.time()

    def _send_new(self, text):
        try:
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            data = urllib.parse.urlencode({"chat_id": self.chat_id, "text": text, "parse_mode": "HTML"}).encode()
            with urllib.request.urlopen(urllib.request.Request(url, data=data)) as response:
                return json.loads(response.read())['result']['message_id']
        except: return None

    def _edit_msg(self, text):
        try:
            url = f"https://api.telegram.org/bot{self.token}/editMessageText"
            data = urllib.parse.urlencode({
                "chat_id": self.chat_id, 
                "message_id": self.message_id, 
                "text": text, 
                "parse_mode": "HTML"
            }).encode()
            urllib.request.urlopen(urllib.request.Request(url, data=data))
        except: pass

# Global Terminal Instance
terminal = TelegramTerminal()

# Custom Log Function
def log(msg):
    print(msg) # GitHub Console ke liye
    sys.stdout.flush()
    terminal.log_stream(msg) # Phone ke liye

# --- LOGIC ---

def get_prompt_content():
    """
    3-Level Smart Search for master_prompt.txt
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # 1. Script ke folder mein
    path1 = os.path.join(script_dir, PROMPT_FILE_NAME)
    # 2. Current Directory mein
    path2 = os.path.join(os.getcwd(), PROMPT_FILE_NAME)
    # 3. Parent Directory mein
    path3 = os.path.join(os.path.dirname(script_dir), PROMPT_FILE_NAME)

    final_path = None
    if os.path.exists(path1): final_path = path1
    elif os.path.exists(path2): final_path = path2
    elif os.path.exists(path3): final_path = path3
        
    if final_path:
        log(f"📄 Found Prompt File: {os.path.basename(final_path)}")
        try:
            with open(final_path, 'r', encoding='utf-8') as f:
                return f.read()
        except Exception as e:
            log(f"⚠️ Read Error: {e}")
            return None
    else:
        log(f"❌ Error: '{PROMPT_FILE_NAME}' not found anywhere!")
        return None

def find_index_json():
    if not os.path.exists(OUTPUT_FOLDER_NAME):
        return None
    for root, dirs, files in os.walk(OUTPUT_FOLDER_NAME):
        for file in files:
            if file.endswith("_index.json"):
                return os.path.join(root, file)
    return None

def generate_config(index_path, master_prompt):
    terminal.update_progress(60, "Reading Index...")
    try:
        with open(index_path, 'r') as f:
            index_data = json.load(f)
        
        terminal.update_progress(80, "Applying Logic...")
        book_name = os.path.basename(index_path).replace("_index.json", "")
        
        config = {
            "book_id": str(int(time.time())),
            "book_name": book_name,
            "exam_target": "General_Competition", 
            "subject": "General_Studies",       
            "total_chapters": len(index_data),
            "generation_mode": "FAST",
            "prompt_template": master_prompt
        }
        
        output_dir = os.path.dirname(index_path)
        config_path = os.path.join(output_dir, CONFIG_FILE_NAME)
        
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=4)
            
        return config_path

    except Exception as e:
        log(f"❌ Error generating config: {e}")
        return None

def main():
    terminal.start()
    
    # 1. Load Prompt
    terminal.update_progress(10, "Loading Prompt...")
    log("🔍 Searching for Master Prompt...")
    prompt_content = get_prompt_content()
    
    if not prompt_content:
        terminal.update_progress(0, "❌ FILE ERROR")
        log("STOP: Master Prompt file missing.")
        return

    # 2. Find Index
    terminal.update_progress(30, "Locating Index...")
    index_path = find_index_json()
    
    if not index_path:
        terminal.update_progress(0, "❌ INDEX MISSING")
        log("❌ Error: Phase 1 output not found.")
        return

    log(f"✅ Found Index: {os.path.basename(index_path)}")
    
    # 3. Generate Config
    config_path = generate_config(index_path, prompt_content)
    
    if config_path:
        terminal.update_progress(100, "✅ CONFIG SAVED")
        log(f"🎉 Config Ready at: {config_path}")
        terminal.log_stream("System Ready for Phase 3.")
    else:
        terminal.update_progress(0, "❌ GEN FAILED")

if __name__ == "__main__":
    main()
