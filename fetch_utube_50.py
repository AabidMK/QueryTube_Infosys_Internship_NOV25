import os
import pandas as pd
from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled, NoTranscriptFound, VideoUnavailable
from youtube_transcript_api.formatters import TextFormatter
import time
import random

# --- Config ---
input_csv = "final_output.csv"
output_csv = "final_merged_output.csv"
failed_csv = "failed_videos.csv"

MIN_DELAY = 5
MAX_DELAY = 15

# --- Load metadata file ---
df = pd.read_csv(input_csv)
all_video_ids = df["id"].astype(str).tolist()
print(f"Found {len(all_video_ids)} videos in file")

# --- Resume support ---
transcript_map = {}
failed_list = []

if os.path.exists(output_csv):
    old = pd.read_csv(output_csv)
    if "id" in old.columns and "transcript" in old.columns:
        # Convert all transcripts to strings to avoid AttributeError
        transcript_map = {str(k): str(v) if not pd.isna(v) else "" for k, v in zip(old["id"], old["transcript"])}
        print(f"Loaded {len(transcript_map)} existing transcripts")

if os.path.exists(failed_csv):
    ff = pd.read_csv(failed_csv)
    failed_list = ff["id"].astype(str).tolist()
    print(f"Loaded {len(failed_list)} failed videos")

# --- Filter videos: only missing transcripts ---
remaining = [
    vid for vid in all_video_ids
    if (vid not in transcript_map or str(transcript_map.get(vid, "")).strip() == "")
    and vid not in failed_list
]
print(f"Remaining to process: {len(remaining)}")

# --- Initialize ---
ytt = YouTubeTranscriptApi()
formatter = TextFormatter()

# --- Process each video ---
for i, vid in enumerate(remaining, start=1):
    print(f"[{i}/{len(remaining)}] {vid}")

    try:
        transcript_list = ytt.list(vid)

        try:
            transcript = transcript_list.find_manually_created_transcript(['en'])
        except:
            transcript = transcript_list.find_generated_transcript(['en'])

        text = formatter.format_transcript(transcript.fetch().snippets)
        transcript_map[vid] = text

        print(f"   Success ({len(text)} chars)")

    except TranscriptsDisabled:
        print("   Transcripts disabled")
        failed_list.append(vid)

    except NoTranscriptFound:
        print("   No transcript found")
        failed_list.append(vid)

    except VideoUnavailable:
        print("   Video unavailable")
        failed_list.append(vid)

    except Exception as e:
        msg = str(e).lower()
        if any(x in msg for x in ["block", "429", "forbidden", "too many"]):
            print("   IP blocked — STOP NOW")
            failed_list.append(vid)
            break
        else:
            print("   Error:", e)
            failed_list.append(vid)

    # --- Save progress safely using temporary file ---
    merged = df.copy()
    merged["transcript"] = merged["id"].astype(str).map(lambda x: str(transcript_map.get(x, "")))

    temp_output = output_csv + "_temp"
    merged.to_csv(temp_output, index=False, encoding="utf-8")
    os.replace(temp_output, output_csv)  # Atomically replace the original file

    pd.DataFrame({"id": failed_list}).to_csv(failed_csv, index=False)

    time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

# --- Final Summary ---
print("\nDONE!")
print("Successful transcripts:", len(transcript_map))
print("Failed videos:", len(failed_list))
