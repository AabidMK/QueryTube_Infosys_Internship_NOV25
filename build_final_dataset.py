import pandas as pd
import re
import isodate

# -------------------------------
# 1️⃣ Load both CSV files
# -------------------------------
mentor_df = pd.read_csv("cleaned_youtube_data.csv")  # mentor dataset
your_df   = pd.read_csv("merged_youtube_videos_with_transcripts.csv")  # your 50 videos + transcripts

print("Mentor DF shape:", mentor_df.shape)
print("Your DF shape:", your_df.shape)

# -------------------------------
# 2️⃣ Vertical merge (stack rows)
# -------------------------------
combined_df = pd.concat([mentor_df, your_df], axis=0, ignore_index=True)
print("Combined shape:", combined_df.shape)

# -------------------------------
# 3️⃣ Clean text columns (if exist)
# -------------------------------
def clean_text(text):
    if pd.isna(text):
        return ""
    text = str(text).lower()
    text = re.sub(r'[^a-z0-9\s]', " ", text)
    text = re.sub(r'\s+', " ", text).strip()
    return text

text_columns = ["title", "description", "transcript"]

for col in text_columns:
    if col in combined_df.columns:
        combined_df[col] = combined_df[col].apply(clean_text)

# Ensure transcript column exists
if "transcript" not in combined_df.columns:
    combined_df["transcript"] = ""

# -------------------------------
# 4️⃣ Convert duration to seconds
# -------------------------------
def convert_duration_to_seconds(d):
    if pd.isna(d):
        return None
    try:
        return int(isodate.parse_duration(str(d)).total_seconds())
    except:
        return None

if "duration" in combined_df.columns:
    combined_df["duration_seconds"] = combined_df["duration"].apply(convert_duration_to_seconds)
else:
    combined_df["duration_seconds"] = None

# -------------------------------
# 5️⃣ Drop exact duplicates (optional but nice)
# -------------------------------
combined_df = combined_df.drop_duplicates()

# -------------------------------
# 6️⃣ Save output
# -------------------------------
combined_df.to_csv("final_output.csv", index=False, encoding="utf-8")
print("✅ Done! final_output.csv created successfully.")
print("Final shape:", combined_df.shape)

