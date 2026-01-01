import chromadb
import os
from dotenv import load_dotenv
from google import genai

CHROMA_PATH = "QueryTube_db"
COLLECTION_NAME = "youtube_videos_metadata"
MODEL_NAME = "gemini-2.5-flash"

load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

client_llm = genai.Client(api_key=GEMINI_API_KEY)

def summarize_video(video_id: str) -> str:
    client = chromadb.PersistentClient(path=CHROMA_PATH)
    collection = client.get_collection(name=COLLECTION_NAME)

    result = collection.get(
        ids=[video_id],
        include=["documents"]
    )

    if not result["documents"]:
        raise ValueError("Transcript not found")

    transcript = result["documents"][0]

    prompt = f"""You are an expert technical content summarizer.

    Summarize the following YouTube video transcript in a well-structured format.

    Use this structure:

    📌 Video Overview
    - Brief description of the video (2–3 lines)

    🧠 Key Concepts Explained
    - Bullet points of main ideas

    🛠️ Tools / Technologies Mentioned
    - Tools, frameworks, or technologies (if any)

    🎯 Key Takeaways
    - 3–5 concise takeaways

    Transcript:
    {transcript}
    """

    response = client_llm.models.generate_content(
        model=MODEL_NAME,
        contents=prompt
    )

    return response.text.strip()


if __name__ == "__main__":
    video_id = input("Enter video id: ").strip()

    try:
        summary = summarize_video(video_id)
        print("\n✅ VIDEO SUMMARY:\n")
        print(summary)
    except Exception as e:
        print("\n❌ Error:", e)