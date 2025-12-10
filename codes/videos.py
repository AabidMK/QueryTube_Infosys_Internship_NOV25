import os
import pandas as pd
from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled, NoTranscriptFound, VideoUnavailable
from youtube_transcript_api.formatters import TextFormatter
import time
import random

# --- Config ---
csv_path = "output.csv"
output_path = "transcripts_output.csv"
failed_path = "failed_videos.csv"
MIN_DELAY = 5
MAX_DELAY = 15

# --- Load dataset ---
df = pd.read_csv(csv_path)
all_video_ids = df['id'].dropna().unique().tolist()
print(f"Found {len(all_video_ids)} videos in dataset")

def safe_read_csv(filepath):
    """Return an empty DataFrame if file missing or empty, otherwise read CSV."""
    if not os.path.exists(filepath) or os.path.getsize(filepath) == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(filepath)
    except pd.errors.EmptyDataError:
        # File exists but contains no data/columns
        return pd.DataFrame()

# --- Load already processed videos ---
already_fetched = set()
permanent_failures = set()

if os.path.exists(output_path):
    existing_df = safe_read_csv(output_path)
    if not existing_df.empty and 'video_id' in existing_df.columns and 'transcript' in existing_df.columns:
        success_df = existing_df[existing_df['transcript'].notna() & (existing_df['transcript'] != '')]
        already_fetched = set(success_df['video_id'].astype(str).tolist())
        print(f"Already fetched: {len(already_fetched)} videos")

if os.path.exists(failed_path):
    failed_df = safe_read_csv(failed_path)
    if not failed_df.empty and 'reason' in failed_df.columns:
        perm_fail_df = failed_df[failed_df['reason'].isin(['disabled', 'not_found', 'unavailable'])]
        permanent_failures = set(perm_fail_df['video_id'].astype(str).tolist())
        print(f"Permanently failed: {len(permanent_failures)} videos")

# --- Filter videos to process ---
videos_to_process = [
    vid for vid in all_video_ids 
    if str(vid) not in already_fetched and str(vid) not in permanent_failures
]
print(f"Videos to process: {len(videos_to_process)}")

if len(videos_to_process) == 0:
    print("All videos already processed!")
    exit()

# --- Initialize ---
ytt_api = YouTubeTranscriptApi()
formatter = TextFormatter()

# initialize the data lists safely
transcripts_data = safe_read_csv(output_path).to_dict('records') if os.path.exists(output_path) else []
failed_data = safe_read_csv(failed_path).to_dict('records') if os.path.exists(failed_path) else []
# --- Load already processed videos ---

# --- Fetch transcripts ---
for i, vid in enumerate(videos_to_process, start=1):
    vid_str = str(vid)
    print(f"[{i}/{len(videos_to_process)}] {vid_str}")
    
    try:
        transcript_list = ytt_api.list(vid_str)
        try:
            transcript = transcript_list.find_manually_created_transcript(['en'])
        except:
            transcript = transcript_list.find_generated_transcript(['en'])
        
        transcript_text = formatter.format_transcript(transcript.fetch().snippets)
        transcripts_data.append({'video_id': vid_str, 'transcript': transcript_text})
        print(f"   Success ({len(transcript_text)} chars)")

    except TranscriptsDisabled:
        print(f"   Transcripts disabled")
        failed_data.append({'video_id': vid_str, 'reason': 'disabled'})

    except (NoTranscriptFound, Exception) as e:
        error_msg = str(e).lower()
        if any(x in error_msg for x in ['blocking', 'too many', 'rate limit', 'forbidden', '429']):
            print(f"   IP blocked - stopping")
            failed_data.append({'video_id': vid_str, 'reason': 'ip_blocked'})
            break
        else:
            print(f"   Failed: {str(e)[:60]}")
            failed_data.append({'video_id': vid_str, 'reason': 'not_found'})

    except VideoUnavailable:
        print(f"   Video unavailable")
        failed_data.append({'video_id': vid_str, 'reason': 'unavailable'})

    # Save progress
    pd.DataFrame(transcripts_data).to_csv(output_path, index=False, encoding='utf-8')
    pd.DataFrame(failed_data).to_csv(failed_path, index=False, encoding='utf-8')
    
    if i < len(videos_to_process):
        time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

# --- Summary ---
print(f"\nDone. Success: {len(transcripts_data)}, Failed: {len(failed_data)}")