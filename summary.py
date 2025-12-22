import lancedb
import subprocess

# ---------- CONFIG ----------
DB_PATH = "lancedb_videos"
TABLE_NAME = "videos"
OLLAMA_MODEL = "qwen2:0.5b"


def get_transcript(video_id):
    db = lancedb.connect(DB_PATH)
    table = db.open_table(TABLE_NAME)

    rows = table.search().limit(1000).to_list()

    for row in rows:
        payload = row.get("payload", {})
        if payload.get("video_id") == video_id:
            return payload.get("transcript")

    return None


def summarize_with_ollama(text):
    prompt = f"""
Summarize the following video transcript in 5–6 clear bullet points:

{text}
"""

    result = subprocess.run(
        ["ollama", "run", OLLAMA_MODEL],
        input=prompt,
        text=True,
        capture_output=True,
        encoding="utf-8",
        errors="ignore"
    )

    return result.stdout.strip()


def main():
    video_id = input("Enter Video ID: ").strip()

    transcript = get_transcript(video_id)

    if not transcript:
        print("❌ Transcript not found for this Video ID")
        return

    print("\n✅ Transcript found. Generating summary...\n")

    summary = summarize_with_ollama(transcript)

    print("📌 SUMMARY:\n")
    print(summary)


if __name__ == "__main__":
    main()