import pandas as pd
import re
import isodate

# -------------------------------
# 1️⃣ Load both CSV files
# -------------------------------
df1 = pd.read_csv("cleaned_youtube_data.csv")
df2 = pd.read_csv("merged_youtube_videos_with_transcripts.csv")

print("DF1 Columns:", df1.columns.tolist())
print("DF2 Columns:", df2.columns.tolist())

# -------------------------------
# 2️⃣ Vertical merge (stack rows)
# -------------------------------
combined_df = pd.concat([df1, df2], axis=0, ignore_index=True)

# -------------------------------
# 3️⃣ Clean text columns (if exist)
# -------------------------------
def clean_text(text):
    if pd.isna(text):
        return ""
    text = text.lower()
    text = re.sub(r'[^a-z0-9\s]', " ", text)
    text = re.sub(r'\s+', " ", text).strip()
    return text

text_columns = ["title", "description", "transcript"]

for col in text_columns:
    if col in combined_df.columns:
        combined_df[col] = combined_df[col].apply(clean_text)

# -------------------------------
# 4️⃣ Clean transcript (if exists)
# -------------------------------
if "transcript" not in combined_df.columns:
    combined_df["transcript"] = ""

# -------------------------------
# 5️⃣ Convert duration to seconds
# -------------------------------
def convert_duration_to_seconds(d):
    if pd.isna(d):
        return None
    try:
        return int(isodate.parse_duration(d).total_seconds())
    except:
        return None

if "duration" in combined_df.columns:
    combined_df["duration_seconds"] = combined_df["duration"].apply(convert_duration_to_seconds)
else:
    combined_df["duration_seconds"] = None

# -------------------------------
# 6️⃣ Save output
# -------------------------------
combined_df.to_csv("final_output.csv", index=False)

print("Done! final_output.csv created successfully.")
