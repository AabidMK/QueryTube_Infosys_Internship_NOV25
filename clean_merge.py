import pandas as pd
import re

# === INPUT FILES ===
main_csv = "videos_with_transcripts.csv"     # your merged metadata + transcripts
extra_csv = "cleaned_youtube_data.csv"              # the new dataset given to you
output_csv = "final_clean_dataset.csv"

# === STEP 1: Load datasets ===
df_main = pd.read_csv(main_csv)
df_extra = pd.read_csv(extra_csv)

# === STEP 2: Merge datasets on video ID ===
# Ensure both have a common column name
if "video_id" in df_extra.columns:
    df_extra = df_extra.rename(columns={"video_id": "id"})

merged = df_main.merge(df_extra, on="id", how="left")

# === STEP 3: Clean column names ===
def clean_column(col):
    col = col.lower()
    col = re.sub(r'[^a-z0-9_]+', '_', col)   # replace special chars with _
    col = re.sub(r'_+', '_', col)            # collapse multiple underscores
    return col.strip('_')

merged.columns = [clean_column(c) for c in merged.columns]

# === STEP 4: Clean title and transcript text ===

# 4a. Detect title column (title_x or title_y)
title_col = None
for col in merged.columns:
    if col.startswith("title"):
        title_col = col
        break

if title_col:
    merged["title"] = (
        merged[title_col]
        .astype(str)
        .str.lower()
        .str.replace(r'[^a-z0-9\s]', ' ', regex=True)
        .str.replace(r'\s+', ' ', regex=True)
        .str.strip()
    )
else:
    print("WARNING: No title column found. Skipping title cleaning.")


# 4b. Detect transcript column (transcript_x or transcript_y)
transcript_col = None
for col in merged.columns:
    if col.startswith("transcript"):
        transcript_col = col
        break

if transcript_col:
    merged["transcript"] = (
        merged[transcript_col]
        .astype(str)
        .str.lower()
        .str.replace(r'[^a-z0-9\s]', ' ', regex=True)
        .str.replace(r'\s+', ' ', regex=True)
        .str.strip()
    )
else:
    print("WARNING: No transcript column found. Skipping transcript cleaning.")



# === STEP 5: Remove duplicates ===
merged = merged.drop_duplicates(subset=["id"], keep="first")

# === STEP 6: Convert ISO8601 duration to seconds ===
def iso8601_to_seconds(duration):
    """
    Convert YouTube ISO8601 duration (e.g., PT1H2M30S) to seconds.
    """
    if pd.isna(duration):
        return None

    pattern = r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?'
    match = re.match(pattern, duration)

    if not match:
        return None

    hours = int(match.group(1)) if match.group(1) else 0
    minutes = int(match.group(2)) if match.group(2) else 0
    seconds = int(match.group(3)) if match.group(3) else 0

    return hours * 3600 + minutes * 60 + seconds

# === STEP 6: Convert ISO8601 duration to seconds ===
duration_col = None

# Find any column that contains "duration" but is not already "duration_seconds"
for col in merged.columns:
    if "duration" in col and "seconds" not in col:
        duration_col = col
        break

if duration_col:
    print(f"Using duration column: {duration_col}")
    merged["duration_seconds"] = merged[duration_col].apply(iso8601_to_seconds)
else:
    print("WARNING: No duration column found. Skipping duration conversion.")


# === STEP 7: Save final output ===
merged.to_csv(output_csv, index=False, encoding="utf-8")
print(f"✅ Final cleaned dataset saved to {output_csv}")
