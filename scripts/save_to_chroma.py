# save_to_chroma.py
import pandas as pd, json
import chromadb
from chromadb.config import Settings

df = pd.read_csv("final_output_with_embeddings.csv")

def load_emb(e):
    if pd.isna(e): return []
    return json.loads(e) if isinstance(e, str) else e

ids, embeddings, metadatas, documents = [], [], [], []
for _, row in df.iterrows():
    vid = str(row.get("id") or row.get("video_id") or "")
    emb = load_emb(row["embedding"])
    title = row.get("title","")
    transcript = row.get("transcript","")
    channel = row.get("channel_title","")
    view_count = row.get("view_count", row.get("viewCount", None))
    duration = row.get("duration_seconds", row.get("duration", None))

    ids.append(vid)
    embeddings.append(emb)
    metadatas.append({
        "video_id": vid,
        "title": title,
        "channel_title": channel,
        "view_count": None if pd.isna(view_count) or view_count=="" else int(view_count),
        "duration": None if pd.isna(duration) or duration=="" else int(duration)
    })
    documents.append(transcript if isinstance(transcript, str) else "")

client = chromadb.Client(Settings(chroma_db_impl="duckdb+parquet", persist_directory="./chroma_db"))
collection_name = "youtube_videos"
if collection_name in [c.name for c in client.list_collections()]:
    coll = client.get_collection(collection_name)
else:
    coll = client.create_collection(name=collection_name, metadata={"source":"CrashCourse_project"})

coll.upsert(ids=ids, documents=documents, metadatas=metadatas, embeddings=embeddings)
client.persist()
print(f"Saved {len(ids)} vectors to Chroma collection '{collection_name}'")
