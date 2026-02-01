import os
import json
import zstandard as zstd
import google.generativeai as genai
from concurrent.futures import ThreadPoolExecutor
from firebase_admin import db
import re

class EliteMatrixWorker:
    def __init__(self, task_file):
        with open(task_file, 'r') as f:
            self.task_data = json.load(f)
        self.keys = os.getenv(f"KEYS_VM_{os.getenv('WORKER_ID')}").split(",")
        self.compressor = zstd.ZstdCompressor(level=3)

    # 🧠 DSA: Recursive JSON Repair Logic
    def recursive_repair(self, raw_text, attempt=1):
        try:
            # Cleaning the markdown junk
            clean_text = re.sub(r'```json|```', '', raw_text).strip()
            return json.loads(clean_text)
        except Exception:
            if attempt < 3:
                console.print(f"[yellow]⚠️ Attempt {attempt} failed. Repairing JSON...[/yellow]")
                # Yahan hum Gemini ko wapas bolte hain ki "Format theek karo"
                # Ya fir Regex se trailing commas aur brackets theek karte hain
                fixed_text = self.fix_json_syntax(clean_text) 
                return self.recursive_repair(fixed_text, attempt + 1)
            return None

    def fix_json_syntax(self, text):
        """Simple Regex Repair for common AI mistakes"""
        text = text.strip()
        if not text.endswith("]"): text += "]"
        if not text.startswith("["): text = "[" + text
        return text

    # 🚀 DevOps: Multi-threaded Firing (The Producer-Consumer Logic)
    def fire_engine(self, topic_data):
        # 8 Keys rotate karne ke liye logic
        key_index = (topic_data['index']) % len(self.keys)
        genai.configure(api_key=self.keys[key_index])
        model = genai.GenerativeModel('gemini-2.0-flash')

        # Logic for Method 1 (Image) vs Method 2 (Text)
        if self.task_data['METHOD'] == 1:
            # Image based firing logic
            pass 
        else:
            # Brain based firing logic
            prompt = f"Topic: {topic_data['topic']} | Master Prompt: {self.task_data.get('MASTER_PROMPT', '')}"
            response = model.generate_content(prompt)
            
        # Recursive repair call
        mcqs = self.recursive_repair(response.text)
        if mcqs:
            self.pack_and_sync(topic_data['topic'], mcqs)

    def pack_and_sync(self, topic, data):
        """DSA: Protobuf + Zstd logic"""
        # Data ko binary (Protobuf simulated) format mein pack karna
        packed_str = "|".join([str(v) for v in data]) 
        compressed = self.compressor.compress(packed_str.encode('utf-8'))
        
        # Firebase Upload
        db.reference(f"Syllabus/{self.task_data['SUBJECT']}/{topic}").update({
            "status": "COMPLETED",
            "payload": compressed.hex()
        })

    def start_matrix(self):
        # 🧵 DevOps: ThreadPool for parallel execution inside ONE VM
        # 8 threads chalenge kyunki 8 keys hain per worker
        topics_with_index = [{"index": i, "topic": t} for i, t in enumerate(self.task_data['BATCH'])]
        
        with ThreadPoolExecutor(max_workers=8) as executor:
            executor.map(self.fire_engine, topics_with_index)
