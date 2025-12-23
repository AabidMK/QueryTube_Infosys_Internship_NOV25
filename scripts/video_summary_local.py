import os
import pandas as pd
import subprocess

# ============================
# PATH SETUP
# ============================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TRANSCRIPT_CSV = os.path.join(BASE_DIR, "data", "transcripts_output.csv")

# ============================
# LOAD TRANSCRIPTS
# ============================
if not os.path.exists(TRANSCRIPT_CSV):
    raise FileNotFoundError("transcripts_output.csv not found")

df = pd.read_csv(TRANSCRIPT_CSV)

video_id = input("Enter video ID to summarize: ").strip()

row = df[df["video_id"] == video_id]

if row.empty or not isinstance(row.iloc[0]["transcript"], str):
    print("❌ Transcript not found for this video.")
    exit()

transcript = row.iloc[0]["transcript"]

# ============================
# STEP 4: LIMIT TRANSCRIPT SIZE (SPEED BOOST)
# ============================
MAX_CHARS = 2000   # reduces inference time drastically
transcript = transcript[:MAX_CHARS]

# ============================
# PROMPT
# ============================
prompt = f"""
Summarize the key ideas from this transcript in bullet points:

{transcript}
"""

print("\n🧠 Sending transcript to local LLM (Ollama - Mistral)")
print("⏳ This should take ~10–25 seconds on CPU. Please wait...\n")

# ============================
# CALL OLLAMA (FAST MODEL)
# ============================
result = subprocess.run(
    ["ollama", "run", "phi"],   # FAST model
    input=prompt.encode("utf-8"),
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE
)

summary = result.stdout.decode("utf-8", errors="ignore").strip()

# ============================
# OUTPUT
# ============================
if summary:
    print("\n📝 Video Summary:\n")
    print(summary)
else:
    print("⚠️ Ollama did not return output.")
    if result.stderr:
        print("\nError details:\n")
        print(result.stderr.decode("utf-8", errors="ignore"))
