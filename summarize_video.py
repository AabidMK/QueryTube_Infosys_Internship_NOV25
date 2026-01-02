import chromadb
import subprocess

# ============================
# Step 1 — Connect to ChromaDB
# ============================
client = chromadb.PersistentClient(path="chroma_store")  # same path as Task 5
collection = client.get_collection("youtube_videos")

# ============================
# Step 2 — Fetch transcript by video ID
# ============================
def get_transcript(video_id):
    results = collection.get(ids=[video_id])
    if results and results["metadatas"]:
        metadata = results["metadatas"][0]
        transcript = metadata.get("transcript", None)
        return transcript, metadata
    return None, None

# ============================
# Step 3 — Summarize with Ollama (local LLM)
# ============================
def summarize_with_ollama(transcript):
    prompt = f"Summarize the following transcript in 5-6 sentences:\n\n{transcript}"
    result = subprocess.run(
        ["ollama", "run", "llama2"],  # replace 'llama2' with 'mistral', 'gemma', etc.
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

    if not transcript or transcript == "nan":
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
