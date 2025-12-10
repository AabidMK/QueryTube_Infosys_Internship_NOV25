import pandas as pd
import re

# ============================
# 1. LOAD FILES
# ============================

my_df = pd.read_csv("final_merged.csv")
sir_df = pd.read_csv("cleaned_youtube_data.csv")

print("My file:", my_df.shape)
print("Sir file:", sir_df.shape)

# ============================
# 2. ADD MISSING COLUMNS TO MATCH SIR'S FORMAT
# ============================

# --- TRUE/FALSE transcript availability ---
if "is_transcript_available" not in my_df.columns:
    my_df["is_transcript_available"] = my_df["transcript"].notna()

# --- duration_seconds (compute only for your file) ---
def iso_to_seconds(duration):
    if pd.isna(duration):
        return None

    hours = minutes = seconds = 0
    h = re.search(r"(\d+)H", duration)
    m = re.search(r"(\d+)M", duration)
    s = re.search(r"(\d+)S", duration)

    if h: hours = int(h.group(1))
    if m: minutes = int(m.group(1))
    if s: seconds = int(s.group(1))

    return hours * 3600 + minutes * 60 + seconds

if "duration_seconds" not in my_df.columns:
    my_df["duration_seconds"] = my_df["duration"].apply(iso_to_seconds)

# ============================
# 3. CLEAN title + transcript
# ============================

def clean_text(s):
    if pd.isna(s):
        return s
    s = s.lower()
    s = re.sub(r"[^a-z0-9\s]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

my_df["title"] = my_df["title"].astype(str).apply(clean_text)
my_df["transcript"] = my_df["transcript"].astype(str).apply(clean_text)

# ============================
# 4. REORDER COLUMNS AS SIR'S FILE
# ============================

sir_columns = sir_df.columns.tolist()
my_df = my_df.reindex(columns=sir_columns)

# ============================
# 5. MERGE (YOUR FILE INTO SIR'S FILE)
# ============================

final_df = pd.concat([sir_df, my_df], ignore_index=True)
print("After merge:", final_df.shape)

# ============================
# 6. REMOVE DUPLICATES BY id
# ============================

before = final_df.shape[0]
final_df = final_df.drop_duplicates(subset=["id"], keep="first")
after = final_df.shape[0]
print(f"Duplicates removed: {before - after}")

# ============================
# 7. SAVE FINAL OUTPUT
# ============================

output_path = "C:/Users/MADHAV/OneDrive/Desktop/ted-ed/task-3/final_cleaned_merged.csv"
final_df.to_csv(output_path, index=False, encoding="utf-8")

print("\n🎉 FINAL MERGED FILE SAVED AT:", output_path)
