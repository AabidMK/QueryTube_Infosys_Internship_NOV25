import pandas as pd
import re

# -------------------------------
# CONFIG
# -------------------------------
INPUT_CSV = "merged_output.csv"       # your merged dataset
OUTPUT_CSV = "final_clean_dataset.csv"    # cleaned output


# -------------------------------
# CLEANING FUNCTIONS
# -------------------------------

def clean_text(text):
    """Lowercase + remove special characters except spaces and basic punctuation."""
    if pd.isna(text):
        return ""
    
    text = text.lower()  # lower case
    text = re.sub(r"[^a-z0-9\s\.,!?]", " ", text)  # remove special chars except . , ! ?
    text = re.sub(r"\s+", " ", text).strip()  # fix spacing
    return text


# -------------------------------
# LOAD DATA
# -------------------------------
df = pd.read_csv(INPUT_CSV)

print("Original Shape:", df.shape)


# -------------------------------
# DROP DUPLICATES
# -------------------------------
df.drop_duplicates(subset=["id"], inplace=True)
print("After removing duplicate IDs:", df.shape)

# Optional: also remove duplicate titles
df.drop_duplicates(subset=["title"], inplace=True)
print("After removing duplicate titles:", df.shape)


# -------------------------------
# CLEAN TEXT COLUMNS
# -------------------------------
if "title" in df.columns:
    df["title"] = df["title"].astype(str).apply(clean_text)

if "transcript" in df.columns:
    df["transcript"] = df["transcript"].astype(str).apply(clean_text)


# -------------------------------
# SAVE CLEANED DATA
# -------------------------------
df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")

print("\n🎉 CLEANING COMPLETE!")
print(f"Clean dataset saved as: {OUTPUT_CSV}")
print("Final Shape:", df.shape)
