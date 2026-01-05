import os
import time
import random
import pandas as pd
from youtube_transcript_api import (
    YouTubeTranscriptApi,
    TranscriptsDisabled,
    NoTranscriptFound,
    VideoUnavailable,
)
from youtube_transcript_api.formatters import TextFormatter

# -----------------------------
# CONFIG
# -----------------------------
INPUT_CSV = "data/youtube_50_videos.csv"
OUTPUT_CSV = "data/transcripts_output.csv"
FAILED_CSV = "data/failed_videos.csv"


MIN_DELAY = 5
MAX_DELAY = 15

formatter = TextFormatter()

# -----------------------------
# Utility: Safe CSV read
# -----------------------------
def safe_read_csv(path):
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()

# -----------------------------
# Load video IDs
# -----------------------------
df = pd.read_csv(INPUT_CSV)

if "id" not in df.columns:
    raise ValueError("Input CSV must contain an 'id' column")

video_ids = df["id"].dropna().astype(str).unique().tolist()
print(f"Found {len(video_ids)} videos")

# -----------------------------
# Load existing progress
# -----------------------------
success_df = safe_read_csv(OUTPUT_CSV)
failed_df = safe_read_csv(FAILED_CSV)

processed_ids = set(success_df["video_id"]) if not success_df.empty else set()
failed_ids = set(failed_df["video_id"]) if not failed_df.empty else set()

videos_to_process = [
    vid for vid in video_ids
    if vid not in processed_ids and vid not in failed_ids
]

print(f"Videos to process: {len(videos_to_process)}")

# -----------------------------
# Prepare output buffers
# -----------------------------
success_data = success_df.to_dict("records") if not success_df.empty else []
failed_data = failed_df.to_dict("records") if not failed_df.empty else []

# -----------------------------
# Fetch transcripts
# -----------------------------
for i, video_id in enumerate(videos_to_process, start=1):
    print(f"[{i}/{len(videos_to_process)}] {video_id}")

    try:
        transcript = YouTubeTranscriptApi.get_transcript(
            video_id, languages=["en"]
        )

        transcript_text = formatter.format_transcript(transcript)

        if not transcript_text or len(transcript_text.strip()) < 50:
            raise NoTranscriptFound(video_id)

        success_data.append({
            "video_id": video_id,
            "transcript": transcript_text
        })

        print(f"   ✅ Success ({len(transcript_text)} chars)")

    except TranscriptsDisabled:
        print("   ⚠️ Transcripts disabled")
        failed_data.append({"video_id": video_id, "reason": "disabled"})

    except NoTranscriptFound:
        print("   ⚠️ English transcript not found")
        failed_data.append({"video_id": video_id, "reason": "not_found"})

    except VideoUnavailable:
        print("   ⚠️ Video unavailable")
        failed_data.append({"video_id": video_id, "reason": "unavailable"})

    except Exception as e:
        print(f"   ❌ Unexpected error: {str(e)[:60]}")
        failed_data.append({"video_id": video_id, "reason": "unknown_error"})

    # Save progress after each video
    pd.DataFrame(success_data).to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    pd.DataFrame(failed_data).to_csv(FAILED_CSV, index=False, encoding="utf-8")

    time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

# -----------------------------
# Summary
# -----------------------------
print("\nDONE")
print(f"Total success: {len(success_data)}")
print(f"Total failed : {len(failed_data)}")
