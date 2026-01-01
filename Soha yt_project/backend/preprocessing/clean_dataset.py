import pandas as pd
import re
import isodate
import os

BASE = os.path.dirname(os.path.abspath(__file__))

input_file = os.path.join(BASE, "cleaned_youtube_data.csv")
output_file = os.path.join(BASE, "processed_final_dataset.csv")

df = pd.read_csv(input_file)

print("Initial rows:", len(df))


df["title"] = df["title"].astype(str).str.lower()
df["transcript"] = df["transcript"].astype(str).str.lower()

def clean_text(txt):
    txt = re.sub(r"[^a-z0-9\s]", " ", txt)   
    txt = re.sub(r"\s+", " ", txt)           
    return txt.strip()

df["title"] = df["title"].apply(clean_text)
df["transcript"] = df["transcript"].apply(clean_text)

print("✓ Cleaned text")


before = len(df)
df.drop_duplicates(subset=["id"], inplace=True)
after = len(df)

print(f"Removed duplicates: {before - after}")


def convert_duration(val):
    try:
        return int(isodate.parse_duration(val).total_seconds())
    except:
        return None


if "duration_seconds" in df.columns:
    df["duration_seconds"] = df["duration_seconds"].fillna(
        df["duration"].apply(convert_duration)
    )
else:
    df["duration_seconds"] = df["duration"].apply(convert_duration)

print("✓ Duration converted")

df.to_csv(output_file, index=False)

print("\nDONE")
print("Saved file:", output_file)
print("Final rows:", len(df))
