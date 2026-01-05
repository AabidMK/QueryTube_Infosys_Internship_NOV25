import pandas as pd
import subprocess
import sys

# ===============================
# CONFIG
# ===============================
META_CSV_PATH = "vector_db/meta_for_index.csv"
OLLAMA_MODEL = "phi"

# ===============================
# LOAD METADATA CSV
# ===============================
try:
    df = pd.read_csv(META_CSV_PATH)
except FileNotFoundError:
    print("❌ meta_for_index.csv not found. Build vector DB first.")
    sys.exit(1)

required_cols = {"video_id", "title", "transcript"}
if not required_cols.issubset(df.columns):
    print(f"❌ CSV must contain columns: {required_cols}")
    sys.exit(1)

# ===============================
# INPUT
# ===============================
video_id = input("Enter video ID to summarize: ").strip()

row = df[df["video_id"].astype(str) == str(video_id)]

if row.empty:
    print("❌ Video ID not found in metadata.")
    sys.exit(1)

transcript = row.iloc[0]["transcript"]

if not isinstance(transcript, str) or not transcript.strip():
    print("❌ Transcript is empty.")
    sys.exit(1)

# ===============================
# TRIM TRANSCRIPT (FAST)
# ===============================
transcript = transcript[:2000]

prompt = f"""
Summarize the following YouTube video transcript clearly and concisely.
Focus only on key ideas and main takeaways.

Transcript:
{transcript}
"""

# ===============================
# CALL OLLAMA (PHI)
# ===============================
print("\n🧠 Sending transcript to local LLM (Ollama)...")
print("⏳ Please wait (20–40 seconds on CPU)\n")

result = subprocess.run(
    ["ollama", "run", OLLAMA_MODEL],
    input=prompt.encode("utf-8"),
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE
)

summary = result.stdout.decode("utf-8", errors="ignore")

# ===============================
# OUTPUT
# ===============================
print("\n📝 Video Summary:\n")
print(summary.strip())
