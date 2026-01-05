import time
import pandas as pd
from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled, NoTranscriptFound, VideoUnavailable
from youtube_transcript_api.formatters import TextFormatter  
CSV_PATH = "../Tasks/freecodecamp.csv"
t_api = YouTubeTranscriptApi()
formatter = TextFormatter()
df = pd.read_csv(CSV_PATH)

if "transcript" not in df.columns:
    df["transcript"] = None

df["transcript"] = df["transcript"].astype(str)

to_process = df[
    (df["transcript"].isna()) |
    (df["transcript"].str.strip().isin(["None", "none", "NONE", "nan", "NaN", ""]))
]

to_process = to_process.head(10)

for idx, row in to_process.iterrows():
    vid = str(row["id"]).strip()
    try:
        T = t_api.fetch(vid)
        T = formatter.format_transcript(T)
    except Exception as e:
        T = None
    df.at[idx, "transcript"] = str(T)
    time.sleep(3)

df.to_csv(CSV_PATH, index=False)
