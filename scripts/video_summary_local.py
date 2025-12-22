import os
import pandas as pd
import subprocess

# ============================
# Paths
# ============================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TRANSCRIPT_CSV = os.path.join(BASE_DIR, "data", "transcripts_output.csv")

# ============================
# Load transcripts
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
# Prompt
# ============================
prompt = f"""
Summarize the following YouTube video transcript clearly and concisely.

Rules:
- Use bullet points
- Focus only on key ideas and takeaways
- Keep it short and clear

Transcript:
{transcript}
"""

print("\n🧠 Sending transcript to local LLM (Ollama)")
print("⏳ This may take 30–90 seconds on CPU. Please wait...\n")

# ============================
# Call Ollama safely (UTF-8)
# ============================
result = subprocess.run(
    ["ollama", "run", "llama3"],
    input=prompt.encode("utf-8"),
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE
)

summary = result.stdout.decode("utf-8", errors="ignore").strip()

# ============================
# Output
# ============================
if summary:
    print("\n📝 Video Summary:\n")
    print(summary)
else:
    print("⚠️ Ollama did not return output.")
    if result.stderr:
        print("\nError details:\n")
        print(result.stderr.decode("utf-8", errors="ignore"))
