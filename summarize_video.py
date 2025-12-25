import chromadb
import subprocess

# ============================
# Step 1 — Connect to ChromaDB
# ============================
client = chromadb.PersistentClient(path="./chroma_db")
collection = client.get_collection("youtube_videos")

# ============================
# Step 2 — Fetch transcript by video ID
# ============================
def get_transcript(video_id):
    results = collection.get(ids=[video_id])
    if results and results["documents"]:
        transcript = results["documents"][0]
        metadata = results["metadatas"][0]
        return transcript, metadata
    return None, None

# ============================
# Step 3 — Summarize with Ollama
# ============================
def summarize_with_ollama(transcript):
    prompt = f"Summarize the following transcript in 5-6 sentences:\n\n{transcript}"
    result = subprocess.run(
        ["ollama", "run", "llama2"],  # You can replace 'llama2' with 'mistral', 'gemma', etc.
        input=prompt.encode("utf-8"),
        capture_output=True
    )
    return result.stdout.decode("utf-8")

# ============================
# Step 4 — Main flow
# ============================
if __name__ == "__main__":
    video_id = input("Enter Video ID: ").strip()
    transcript, metadata = get_transcript(video_id)

    if not transcript:
        print("❌ Transcript not found for this video ID.")
    else:
        print("\n=== Video Metadata ===")
        print("Video ID:", video_id)
        print("Title:", metadata.get("title", "Unknown"))
        print("Channel:", metadata.get("channel_title", "Unknown"))
        print("View Count:", metadata.get("view_count", "Unknown"))
        print("Duration (seconds):", metadata.get("duration", "Unknown"))

        print("\n=== Summary ===")
        summary = summarize_with_ollama(transcript)
        print(summary)