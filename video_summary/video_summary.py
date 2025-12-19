import os
from dotenv import load_dotenv
from qdrant_client import QdrantClient
from qdrant_client.http import models
from google import genai
from google.genai.types import HttpOptions

# ---------------- CONFIG ----------------
load_dotenv()
API_KEY = os.getenv("GEMINI_API_KEY")
if not API_KEY:
    raise ValueError("GEMINI_API_KEY not found in .env")

# Gemini client (NEW SDK)
genai_client = genai.Client(
    api_key=API_KEY,
    http_options=HttpOptions(api_version="v1beta")
)

QDRANT_URL = "http://localhost:6333"
COLLECTION_NAME = "youtube_videos"
client = QdrantClient(url=QDRANT_URL)

# ---------------- FETCH TRANSCRIPT ----------------
def fetch_transcript(video_id: str) -> str:
    all_text = []
    offset = None

    while True:
        points, offset = client.scroll(
            collection_name=COLLECTION_NAME,
            scroll_filter=models.Filter(
                must=[
                    models.FieldCondition(
                        key="video_id",
                        match=models.MatchValue(value=video_id)
                    )
                ]
            ),
            limit=50,
            offset=offset,
            with_payload=True
        )

        if not points:
            break

        for p in points:
            payload = p.payload
            if "transcript" in payload and payload["transcript"]:
                all_text.append(payload["transcript"])

        if offset is None:
            break

    return " ".join(all_text)

# ---------------- GENERATE SUMMARY ----------------
SUMMARY_PROMPT = """
You are a YouTube video summarizer.

Return ONLY bullet points.
Do NOT add any introduction or explanation text.
Keep the summary within 250 words.

Transcript:
"""


def generate_gemini_content(transcript: str) -> str:
    try:
        return genai_client.models.generate_content(
            model="gemini-2.5-flash",
            contents=SUMMARY_PROMPT + transcript
        ).text
    except Exception:
        return genai_client.models.generate_content(
            model="gemini-pro",
            contents=SUMMARY_PROMPT + transcript
        ).text


if __name__ == "__main__":
    video_id = input("Enter YouTube Video ID: ").strip()

    print("\nFetching transcript from Qdrant...\n")
    transcript = fetch_transcript(video_id)

    if not transcript.strip():
        print("No transcript found for this video ID")
    else:
        print("Transcript fetched successfully\n")
        summary = generate_gemini_content(transcript)
        print("========== SUMMARY ==========")
        print(summary)
        print("=============================")
