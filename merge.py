# import pandas as pd

# videos_csv = "videos_metadata.csv"
# transcripts_csv = "serpapi_transcripts.csv"
# output_csv = "merged_metadata_with_transcripts.csv"

# # Load videos CSV
# videos_df = pd.read_csv(videos_csv)
# print("Videos loaded:", videos_df.shape)

# # Load transcript CSV
# transcripts_df = pd.read_csv(transcripts_csv)
# print("Transcripts loaded:", transcripts_df.shape)

# # Check columns
# print("\nVideos Columns:", videos_df.columns.tolist())
# print("Transcripts Columns:", transcripts_df.columns.tolist())

# # Merge using correct keys
# merged_df = pd.merge(
#     videos_df,
#     transcripts_df,
#     left_on="id",          # column in videos CSV
#     right_on="video_id",   # column in transcripts CSV
#     how="left"
# )

# print("\nMerged shape:", merged_df.shape)

# # Save final CSV
# merged_df.to_csv(output_csv, index=False, encoding="utf-8")

# print(f"\n Merge completed successfully!")
# print(f" Output saved as: {output_csv}")

import pandas as pd

videos_csv = "videos_metadata.csv"
transcripts_csv = "serpapi_transcripts.csv"
output_csv = "merged_metadata_with_transcripts.csv"

# Load videos CSV
videos_df = pd.read_csv(videos_csv)
print("Videos loaded:", videos_df.shape)

# Load transcript CSV
transcripts_df = pd.read_csv(transcripts_csv)
print("Transcripts loaded:", transcripts_df.shape)

# Check columns
print("\nVideos Columns:", videos_df.columns.tolist())
print("Transcripts Columns:", transcripts_df.columns.tolist())

# Merge correctly
merged_df = pd.merge(
    videos_df,
    transcripts_df[["video_id", "transcript"]],  # ensure only transcript column is merged
    left_on="id",
    right_on="video_id",
    how="left"
)

# Remove extra video_id column
merged_df.drop(columns=["video_id"], inplace=True)

print("\nMerged shape:", merged_df.shape)

# Save final CSV
merged_df.to_csv(output_csv, index=False, encoding="utf-8")

print("\n✔ Merge completed successfully!")
print(f"👉 Output saved as: {output_csv}")
