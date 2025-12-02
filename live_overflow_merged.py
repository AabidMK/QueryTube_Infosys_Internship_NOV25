import pandas as pd

def merge_transcript_files():
    
    # File paths
    videos_file = "live_overflow_videos.csv"
    transcripts_file = "live_overflow_transcripts.csv"
    output_file = "live_overflow_merged.csv"
    
    try:
        # Load the data
        df_videos = pd.read_csv(videos_file)
        df_transcripts = pd.read_csv(transcripts_file)
        
        # Ensure the 'id' column exists in both
        if 'id' not in df_videos.columns or 'id' not in df_transcripts.columns:
            print(" Error: 'id' column not found in one of the files.")
            return
        
        merged_df = pd.merge(df_videos, df_transcripts, on='id', how='left')
        
        # Save the merged result
        merged_df.to_csv(output_file, index=False, encoding='utf-8')
        
    except FileNotFoundError as e:
        print(f" File not found: {e}")
    except Exception as e:
        print(f" An error occurred: {e}")

# Run the script
if __name__ == "__main__":
    merge_transcript_files()