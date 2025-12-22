import chromadb
import os
from dotenv import load_dotenv
from google import genai

# ----------------------------------
# CONFIG
# ----------------------------------
CHROMA_PATH = "vector_db"
COLLECTION_NAME = "youtube_videos_collection"
MODEL_NAME = "gemini-2.5-flash"   # or gemini-2.0-flash

# ----------------------------------
# LOAD ENV
# ----------------------------------
load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

if not GEMINI_API_KEY:
    raise RuntimeError(" GEMINI_API_KEY not found in .env")

# ----------------------------------
# INIT CLIENTS
# ----------------------------------
genai_client = genai.Client(api_key=GEMINI_API_KEY)

chroma_client = chromadb.PersistentClient(path=CHROMA_PATH)
collection = chroma_client.get_collection(name=COLLECTION_NAME)

# ----------------------------------
# FETCH TRANSCRIPT
# ----------------------------------
def fetch_transcript(video_id: str) -> str | None:
    result = collection.get(
        ids=[video_id],
        include=["documents"]
    )

    if not result or not result.get("documents"):
        return None

    transcript = result["documents"][0]
    if not transcript or transcript.strip() == "":
        return None

    return transcript.strip()

# ----------------------------------
# MAIN
# ----------------------------------
if __name__ == "__main__":
    video_id = input("Enter video id: ").strip()

    print("\n📥 Fetching transcript from vector DB...")
    transcript = fetch_transcript(video_id)

    if not transcript:
        raise RuntimeError("❌ Transcript not found or empty")

    print("🧠 Generating summary with Gemini...")

    prompt = f"""
You are an expert technical summarizer.

Summarize the following YouTube video transcript clearly and concisely.
Focus on:
- main topics
- key explanations
- important insights

Transcript:
{transcript}
"""

    response = genai_client.models.generate_content(
        model=MODEL_NAME,
        contents=prompt,
        
    )

    print("\n📌 FINAL VIDEO SUMMARY:\n")
    print(response.text.strip())
