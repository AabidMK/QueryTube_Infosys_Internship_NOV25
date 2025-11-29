import os
import pandas as pd
from serpapi import GoogleSearch
import time
import random
from dotenv import load_dotenv

# --- CONFIG ---
INPUT_CSV = "videos_output.csv"
OUTPUT_CSV = "serpapi_transcripts.csv"
FAILED_CSV = "serpapi_failed.csv"

load_dotenv()
SERPAPI_KEY = os.getenv("SERPAPI_API_KEY")

MIN_DELAY = 1.0
MAX_DELAY = 3.0


# ------------------------------------------------------
# CLEAN FORMATTING — Only clean transcript text
# ------------------------------------------------------
def clean_transcript(transcript_items):
    """
    Extract only text from transcript items.
    Removes timestamps and unwanted noises (like [Music]).
    """
    lines = []

    for item in transcript_items:
        snippet = item.get("snippet", "").strip()
        
        # Skip things like [Music], [Applause]
        if snippet.startswith("[") and snippet.endswith("]"):
            continue

        if snippet:
            lines.append(snippet)

    # Convert to one paragraph
    paragraph = " ".join(lines)
    paragraph = " ".join(paragraph.split())  # remove double spaces

    return paragraph.strip()


# ------------------------------------------------------
# SERPAPI TRANSCRIPT FETCHER
# ------------------------------------------------------
def fetch_transcript(video_id):
    # 1️ Try default transcript
    params = {
        "engine": "youtube_video_transcript",
        "v": video_id,
        "language_code": "en",
        "api_key": SERPAPI_KEY
    }

    result = GoogleSearch(params).get_dict()

    # Full transcript available
    if "transcript" in result:
        return clean_transcript(result["transcript"])

    # 2️ Try ASR transcript (auto-generated)
    if "available_transcripts" in result:
        for t in result["available_transcripts"]:
            if t.get("language_code") == "en" and t.get("type") == "asr":
                
                asr_params = {
                    "engine": "youtube_video_transcript",
                    "v": video_id,
                    "language_code": "en",
                    "type": "asr",
                    "api_key": SERPAPI_KEY
                }

                asr_result = GoogleSearch(asr_params).get_dict()

                if "transcript" in asr_result:
                    return clean_transcript(asr_result["transcript"])

    return None


# ------------------------------------------------------
# STEP 1 — Load video list from input CSV
# ------------------------------------------------------
df = pd.read_csv(INPUT_CSV)
video_ids = df["id"].astype(str).tolist()
print(f"Found {len(video_ids)} videos")


# ------------------------------------------------------
# STEP 2 — Resume Support: Load old data
# ------------------------------------------------------
transcripts = {}
failed = set()

# Load previously saved transcripts
if os.path.exists(OUTPUT_CSV):
    old = pd.read_csv(OUTPUT_CSV)
    transcripts = dict(zip(old["video_id"], old["transcript"]))
    print(f"Loaded {len(transcripts)} existing transcripts.")

# Load failed list
if os.path.exists(FAILED_CSV):
    failed_df = pd.read_csv(FAILED_CSV)
    failed = set(failed_df["video_id"].astype(str).tolist())
    print(f"Loaded {len(failed)} failed videos.")


# Remaining videos
remaining = [vid for vid in video_ids if vid not in transcripts and vid not in failed]
print(f"Videos remaining: {len(remaining)}")


# ------------------------------------------------------
# STEP 3 — Fetch transcripts and save to CSV
# ------------------------------------------------------
for i, vid in enumerate(remaining, start=1):
    print(f"\n[{i}/{len(remaining)}] Fetching transcript for {vid}")

    text = fetch_transcript(vid)

    if text:
        transcripts[vid] = text
        print(f"   SUCCESS ({len(text)} chars)")
    else:
        failed.add(vid)
        print("   FAILED (Transcript not found)")

    # SAVE after each video
    pd.DataFrame(
        [{"video_id": v, "transcript": t} for v, t in transcripts.items()]
    ).to_csv(OUTPUT_CSV, index=False, encoding="utf-8")

    pd.DataFrame(
        [{"video_id": v} for v in failed]
    ).to_csv(FAILED_CSV, index=False, encoding="utf-8")

    time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))


# ------------------------------------------------------
# SUMMARY
# ------------------------------------------------------
print("\n=== DONE ===")
print(f"Successful transcripts: {len(transcripts)}")
print(f"Failed transcripts: {len(failed)}")
print(f"Saved to: {OUTPUT_CSV}")
