import os
from dotenv import load_dotenv
from google import genai
from ..vectordb.setup_chroma import get_collection


"""
SUMMARY SERVICE USING GEMINI SDK
"""

# ---------------- ENV + CLIENT ----------------
load_dotenv()
API_KEY = os.getenv("GEMINI_API_KEY")

if not API_KEY:
    raise ValueError("GEMINI_API_KEY not found in .env")

client = genai.Client(api_key=API_KEY)


# ---------------- FETCH TRANSCRIPT + METADATA ----------------
def fetch_video_data(video_id: str):
    collection = get_collection()

    results = collection.query(
        query_texts=["dummy"],
        where={"video_id": video_id},
        n_results=50
    )

    documents = results.get("documents", [[]])[0]
    metadatas = results.get("metadatas", [[]])[0]

    if not documents:
        return "", None

    transcript = " ".join(documents)

    # take metadata from first chunk
    meta = metadatas[0] if metadatas else {}

    return transcript, meta


# ---------------- PROMPT ----------------
PROMPT = """
You are a YouTube video summarizer.

You are a professional YouTube video summarizer.

Return STRICTLY:
- Short bullet points only
- No intro sentence
- No headings
- No emojis
- No paragraphs
- 8–12 meaningful bullet points max
- Each bullet: short + crisp
Transcript:
"""


# ---------------- SUMMARY ----------------
def summarize_text(transcript: str) -> str:

    # handle extremely long transcript
    if len(transcript) > 15000:
        transcript = transcript[:15000]

    response = client.models.generate_content(
        model="models/gemini-2.5-flash",
        contents=PROMPT + transcript
    )

    return response.text


# ---------------- PIPELINE ----------------
def summarize_video(video_id: str):

    transcript, meta = fetch_video_data(video_id)

    if not transcript.strip():
        return {
            "success": False,
            "message": "No transcript found for this video ID"
        }

    summary = summarize_text(transcript)

    return {
        "success": True,
        "video_id": video_id,

        # ---------- metadata ----------
        "title": meta.get("title"),
        "channel": meta.get("channel"),
        "thumbnail": meta.get("thumbnail"),
        "views": meta.get("views"),
        "likes": meta.get("likes"),
        "duration": meta.get("duration"),

        # ---------- summary ----------
        "summary": summary
    }


# ---------------- RUN ----------------
if __name__ == "__main__":
    vid = input("Enter YouTube Video ID: ").strip()
    print("\nFetching transcript from Chroma...")

    result = summarize_video(vid)

    if not result["success"]:
        print("\n❌", result["message"])
    else:
        print("\n========== SUMMARY ==========")
        print(result["summary"])
        print("=============================")
