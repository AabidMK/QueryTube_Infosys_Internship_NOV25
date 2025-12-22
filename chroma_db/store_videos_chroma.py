import chromadb
import pandas as pd
import ast

# --------------------------------
# LOAD CSV
# --------------------------------
CSV_PATH = "../embedding-transcript/title_transcript_embeddings.csv"   
df = pd.read_csv(CSV_PATH)

print("Total rows loaded:", len(df))

# --------------------------------
# SELECT REQUIRED COLUMNS
# --------------------------------
required_cols = [
    "id",
    "transcript",
    "title_transcript_embedding",
    "title",
    "channel_title",
    "viewCount",
    "duration_seconds"
]

missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise ValueError(f"Missing columns in CSV: {missing}")

# --------------------------------
# INIT CHROMA (PERSISTENT)
# --------------------------------
client = chromadb.PersistentClient(path="vector_db")

collection = client.get_or_create_collection(name="youtube_videos_collection")

# --------------------------------
# PREPARE DATA FOR CHROMA
# --------------------------------
ids = df["id"].astype(str).tolist()

documents = df["transcript"].fillna("").astype(str).tolist()

# embeddings are stored as string → convert back to list
embeddings = df["title_transcript_embedding"].apply(
    lambda x: ast.literal_eval(x)
).tolist()
def safe_int(value):
    if pd.isna(value) or value is None:
        return 0
    return int(value)

metadatas = [
    {
        "title": row["title"],
        "channel_title": row["channel_title"],
        "view_count": safe_int(row["viewCount"]),
        "duration": safe_int(row["duration_seconds"])
    }
    for _, row in df.iterrows()
]
# --------------------------------
# UPSERT INTO CHROMA
# --------------------------------
collection.upsert(
    ids=ids,
    documents=documents,
    embeddings=embeddings,
    metadatas=metadatas
)

print(f"✅ Successfully stored {len(ids)} videos in ChromaDB")
