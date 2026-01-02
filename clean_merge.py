import pandas as pd
import re

# === INPUT FILES ===
main_csv = "videos_with_transcripts.csv"    
extra_csv = "cleaned_youtube_data.csv"       
output_csv = "final_clean_dataset.csv"

# === STEP 1: Load datasets ===
df_main = pd.read_csv(main_csv)
df_extra = pd.read_csv(extra_csv)

# === STEP 2: Align column names ===
if "video_id" in df_extra.columns:
    df_extra = df_extra.rename(columns={"video_id": "id"})

# === STEP 3: Concatenate vertically ===
merged = pd.concat([df_main, df_extra], ignore_index=True)

# === STEP 4: Clean column names ===
def clean_column(col):
    col = col.lower()
    col = re.sub(r'[^a-z0-9_]+', '_', col)
    col = re.sub(r'_+', '_', col)
    return col.strip('_')

merged.columns = [clean_column(c) for c in merged.columns]

# === STEP 5: Clean title and transcript values ===
if "title" in merged.columns:
    merged["title"] = (
        merged["title"]
        .astype(str)
        .str.lower()
        .str.replace(r'[^a-z0-9\s]', ' ', regex=True)
        .str.replace(r'\s+', ' ', regex=True)
        .str.strip()
    )

if "transcript" in merged.columns:
    merged["transcript"] = (
        merged["transcript"]
        .astype(str)
        .str.lower()
        .str.replace(r'[^a-z0-9\s]', ' ', regex=True)
        .str.replace(r'\s+', ' ', regex=True)
        .str.strip()
    )

# === STEP 6: Remove duplicates ===
merged = merged.drop_duplicates(subset=["id"], keep="first")

# === STEP 7: Convert ISO8601 duration to seconds ===
def iso8601_to_seconds(duration):
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

if "duration" in merged.columns:
    merged["duration_seconds"] = merged["duration"].apply(iso8601_to_seconds)

# === STEP 8: Save final output ===
merged.to_csv(output_csv, index=False, encoding="utf-8")
print(f"✅ Final cleaned dataset saved to {output_csv} with {len(merged)} rows")
