import pandas as pd
import time
import random
import os
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import TranscriptsDisabled, NoTranscriptFound, VideoUnavailable

def safe_read_csv(filepath):

    if not os.path.exists(filepath) or os.path.getsize(filepath) == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(filepath)
    except:
        return pd.DataFrame()

def fetch_live_overflow_transcripts():
    
    # File paths
    input_file = "live_overflow_videos.csv"
    output_file = "live_overflow_transcripts.csv"
    
    # Step 1: Read input video IDs
    try:
        input_df = pd.read_csv(input_file)
        if 'id' not in input_df.columns:
            print(f" Error: 'id' column not found in {input_file}")
            return
        
        all_video_ids = input_df['id'].astype(str).tolist()
        print(f" Found {len(all_video_ids)} videos")
    except Exception as e:
        print(f" Error reading {input_file}: {e}")
        return
    
    # Step 2: Load existing transcripts
    existing_df = safe_read_csv(output_file)
    
    existing_transcripts = {}
    already_processed = set()
    
    if not existing_df.empty and 'id' in existing_df.columns and 'transcript' in existing_df.columns:
       
        existing_df['id'] = existing_df['id'].astype(str)
        
        for _, row in existing_df.iterrows():
            video_id = str(row['id'])
            transcript = row['transcript']
            existing_transcripts[video_id] = transcript
            
            if pd.notna(transcript) and str(transcript).strip() != "":
                already_processed.add(video_id)
        
        print(f" Already processed: {len(already_processed)} videos")
    
    # Step 3: Identify videos to process
    videos_to_process = []
    
    for video_id in all_video_ids:
        vid_str = str(video_id)
        
        if vid_str in already_processed:
            continue
        
        videos_to_process.append(vid_str)
    
    if len(videos_to_process) == 0:
        print("\n All videos already processed!")
        return
    
    # Step 4: Process videos
    
    for i, video_id in enumerate(videos_to_process, 1):
        print(f"\n [{i}/{len(videos_to_process)}] Processing: {video_id}")
        
        try:
            yt_api = YouTubeTranscriptApi()
            transcript = yt_api.fetch(video_id, languages=['en'])
            transcript_text = ' '.join([snippet.text for snippet in transcript])
            
            if len(transcript_text) > 10000:
                transcript_text = transcript_text[:10000] + "..."
            
            existing_transcripts[video_id] = transcript_text
            print(f" Success ({len(transcript_text)} chars)")
            
        except TranscriptsDisabled:
            existing_transcripts[video_id] = "TRANSCRIPTS_DISABLED"
            print(" Transcripts disabled")
            
        except NoTranscriptFound:
            existing_transcripts[video_id] = "NO_TRANSCRIPT_FOUND"
            print(" No transcript found")
            
        except VideoUnavailable:
            existing_transcripts[video_id] = "VIDEO_UNAVAILABLE"
            print(" Video unavailable")
            
        except Exception as e:
            # Connection error - leave it empty
            existing_transcripts[video_id] = ""
            print(f"  Connection error (leaving empty): {str(e)[:50]}")
        
        result_data = []
        for vid in all_video_ids:
            vid_str = str(vid)
            result_data.append({
                'id': vid_str,
                'transcript': existing_transcripts.get(vid_str, "")
            })
        
        # Save to CSV
        result_df = pd.DataFrame(result_data, columns=['id', 'transcript'])
        result_df.to_csv(output_file, index=False, encoding='utf-8')
        print(" Progress saved")
        
        # Delay between requests (except for last one)
        if i < len(videos_to_process):
            delay = random.randint(1,2)
            print(f" Waiting {delay} seconds...")
            time.sleep(delay)
    
    # Step 5: Final summary
    print("\n" + "=" * 60)
    print("PROCESSING COMPLETE")
    print("=" * 60)
    
    # Load final results for statistics
    final_df = pd.read_csv(output_file)
    
    # Count statistics
    total = len(final_df)
    
    def get_transcript_status(transcript):
        if pd.isna(transcript) or str(transcript).strip() == "":
            return "empty"
        elif transcript == "TRANSCRIPTS_DISABLED":
            return "disabled"
        elif transcript == "NO_TRANSCRIPT_FOUND":
            return "not_found"
        elif transcript == "VIDEO_UNAVAILABLE":
            return "unavailable"
        else:
            return "success"
    
    status_counts = final_df['transcript'].apply(get_transcript_status).value_counts().to_dict()
    
    print(f"   Success : {status_counts.get('success', 0)}")
    
    # List videos with connection errors
    empty_videos = final_df[final_df['transcript'].apply(
        lambda x: pd.isna(x) or str(x).strip() == ""
    )]['id'].tolist()
    
    if empty_videos:
        for vid in empty_videos[:5]:
            print(f"     - {vid}")
        if len(empty_videos) > 5:
            print(f"     ... and {len(empty_videos) - 5} more")

# Run the script
if __name__ == "__main__":
    fetch_live_overflow_transcripts()