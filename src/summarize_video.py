import pickle
import subprocess

# Load metadata (contains transcript)
with open("metadata.pkl", "rb") as f:
    metadata = pickle.load(f)

video_id = input("Enter video ID to summarize: ").strip()

video = next(
    (item for item in metadata if item["video_id"] == video_id),
    None
)

if not video or not video.get("transcript"):
    print("❌ Transcript not found for this video.")
    exit()

transcript = video["transcript"]

prompt = f"""
Summarize the following YouTube video transcript clearly and concisely.
Focus on key ideas and main takeaways.

Transcript:
{transcript}
"""

# Call Ollama (llama3)
result = subprocess.run(
    ["ollama", "run", "llama3"],
    input=prompt.encode("utf-8"),
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE
)

summary = result.stdout.decode("utf-8", errors="ignore")

print("\n📝 Video Summary:\n")
print(summary)


