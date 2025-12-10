import pandas as pd

# --- Input files ---
metadata_csv = "utube_vids.csv"          # your original metadata file
transcripts_csv = "serpapi_transcripts.csv" # transcripts file
output_csv = "videos_with_transcripts.csv"  # final merged file

# --- Load both datasets ---
df_meta = pd.read_csv(metadata_csv)
df_trans = pd.read_csv(transcripts_csv)

# --- Merge on video ID ---
merged = df_meta.merge(df_trans, left_on="id", right_on="video_id", how="left")

# --- Drop duplicate column ---
merged = merged.drop(columns=["video_id"])

# --- Save final merged file ---
merged.to_csv(output_csv, index=False, encoding="utf-8")

print(f"Merged {len(merged)} rows into {output_csv}")
