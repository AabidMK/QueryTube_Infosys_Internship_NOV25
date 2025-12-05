import pandas as pd
import re

input_csv = "merged_metadata_with_transcripts.csv"
output_csv = "cleaned_metadata.csv"

# --------------------------------------------------------
# Function: clean text (lowercase + remove special chars)
# --------------------------------------------------------
def clean_text(text):
    if pd.isna(text):
        return ""
    text = text.lower()
    text = re.sub(r'[^a-z0-9\s]', '', text)     # keep only letters, numbers, spaces
    text = re.sub(r'\s+', ' ', text).strip()    # remove extra spaces
    return text

# --------------------------------------------------------
# Function: Convert ISO 8601 Duration → seconds
# Example: PT1H2M10S → 3730 seconds
# --------------------------------------------------------
def duration_to_seconds(duration):
    if pd.isna(duration):
        return None
    duration = duration.upper()

    hours = minutes = seconds = 0

    h_match = re.search(r'(\d+)H', duration)
    m_match = re.search(r'(\d+)M', duration)
    s_match = re.search(r'(\d+)S', duration)

    if h_match:
        hours = int(h_match.group(1))
    if m_match:
        minutes = int(m_match.group(1))
    if s_match:
        seconds = int(s_match.group(1))

    total_seconds = hours * 3600 + minutes * 60 + seconds
    return total_seconds


# --------------------------------------------------------
# Load CSV
# --------------------------------------------------------
df = pd.read_csv(input_csv)

# --------------------------------------------------------
# Clean title column
# --------------------------------------------------------
if "title" in df.columns:
    df["title"] = df["title"].astype(str).apply(clean_text)

# --------------------------------------------------------
# Clean transcript column
# --------------------------------------------------------
if "transcript" in df.columns:
    df["transcript"] = df["transcript"].astype(str).apply(clean_text)

# --------------------------------------------------------
# Convert duration → seconds
# --------------------------------------------------------
if "duration" in df.columns:
    df["duration_seconds"] = df["duration"].apply(duration_to_seconds)

# --------------------------------------------------------
# Save cleaned CSV
# --------------------------------------------------------
df.to_csv(output_csv, index=False, encoding="utf-8")

print(f"✔ Cleaning completed!")
print(f"✔ New file saved as: {output_csv}")
