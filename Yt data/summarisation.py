import os
from dotenv import load_dotenv
import pandas as pd
from google import genai

# ---------------------------
# LOAD ENV
# ---------------------------
load_dotenv()
API_KEY = os.getenv("GEMINI_API_KEY")
if not API_KEY:
    raise ValueError("❌ GEMINI_API_KEY not found. Check your .env file")

# ---------------------------
# CONFIG
# ---------------------------
CSV_PATH = "youtube_with_embeddings.csv"
GEMINI_MODEL = "gemini-2.5-flash"

# ---------------------------
# LOAD GEMINI
# ---------------------------
client_gemini = genai.Client(api_key=API_KEY)

# ---------------------------
# LOAD CSV
# ---------------------------
print("📂 Loading CSV...")
df = pd.read_csv(CSV_PATH)
print(f"Rows: {len(df)}, Columns: {df.columns.tolist()}")

# ---------------------------
# USER INPUT
# ---------------------------
video_id = input("\nEnter Video ID: ").strip()

# ---------------------------
# FETCH TRANSCRIPT
# ---------------------------
row = df[df["video_id"] == video_id]
if row.empty:
    print("❌ Video ID not found in CSV")
    exit()

transcript = row["transcript_yt"].values[0].strip()
if not transcript:
    print("❌ Transcript not available")
    exit()

print("\n📄 Transcript fetched successfully")

# ---------------------------
# GENERATE SUMMARY
# ---------------------------
prompt = f"""
You are an expert YouTube content summarizer.

Summarize the following video transcript in clear bullet points.
Keep it concise, informative, and beginner-friendly.

Transcript:
{transcript}
"""

response = client_gemini.models.generate_content(
    model=GEMINI_MODEL,
    contents=prompt
)

print("\n📝 VIDEO SUMMARY\n")
print(response.text)
