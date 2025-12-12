# search_faiss_sqlite.py
"""
Search FAISS index + filter by channel_id (uses mapping from final_output_with_embeddings.csv).

How to run:
    python search_faiss_sqlite.py
Then type your query.
"""
import json
import sqlite3
import faiss
import numpy as np
import pandas as pd
from sentence_transformers import SentenceTransformer

# Files
INDEX_FILE = "videos.index"
META_DB = "meta.db"
IDS_FILE = "ids.json"
CSV_FILE = "final_output_with_embeddings.csv"  # contains channel_id mapping

# Set the channel id you want to restrict search to
TARGET_CHANNEL_ID = "UCX6b17PVsYBQ0ip5gyeme-Q"  # CrashCourse ID (change if needed)

# Load FAISS index
index = faiss.read_index(INDEX_FILE)

# Load ids mapping (index -> video_id)
with open(IDS_FILE, "r", encoding="utf-8") as f:
    ids = json.load(f)

# Build a mapping from video_id -> channel_id using CSV (robust column detection)
df_map = pd.read_csv(CSV_FILE, dtype=str).fillna("")
# try common column names for channel id
channel_id_col = None
for c in ["channel_id", "channelId", "channel_id_x", "channel_id_y", "channelId_x", "channelId_y"]:
    if c in df_map.columns:
        channel_id_col = c
        break

if channel_id_col is None:
    # if no explicit channel id column, but video rows have 'channel_title' only,
    # fallback: assume all entries are from desired channel if their channel_title matches
    # (less reliable). Otherwise, we will not filter.
    print("Warning: No explicit channel_id column found in CSV. Channel filtering will try to use channel_title if available.")
    channel_id_col = None

# Create a dict: video_id -> channel_id (or channel_title if channel_id not present)
channel_map = {}
if channel_id_col:
    for _, row in df_map.iterrows():
        vid = str(row.get("id") or row.get("video_id") or "")
        channel_map[vid] = row.get(channel_id_col, "")
else:
    # fallback to channel_title
    if "channel_title" in df_map.columns:
        for _, row in df_map.iterrows():
            vid = str(row.get("id") or row.get("video_id") or "")
            channel_map[vid] = row.get("channel_title", "")
    else:
        # empty mapping
        channel_map = {}

# Open SQLite metadata DB
conn = sqlite3.connect(META_DB)
c = conn.cursor()

# Load embedding model (must be same as used for index)
model = SentenceTransformer("all-MiniLM-L6-v2")

def search(query, k=5):
    # embed query
    qv = model.encode([query]).astype("float32")
    D, I = index.search(qv, k*3)  # fetch a few extra results to allow filtering
    results = []

    for dist, idx in zip(D[0], I[0]):
        if idx < 0 or idx >= len(ids):
            continue
        vid = ids[idx]

        # Check channel filter
        mapped = channel_map.get(vid, "")
        if mapped == "":
            # If we couldn't find a mapping, allow result (or skip - choose behavior)
            # Here we skip to be strict: uncomment next line to allow instead
            # pass
            continue

        # If mapping looks like a channel ID, compare directly
        if mapped.startswith("UC") or mapped.startswith("PL") or mapped.startswith("UCx"):
            if mapped != TARGET_CHANNEL_ID:
                continue
        else:
            # mapped is probably a channel_title; we can compare names (less strict)
            # If TARGET_CHANNEL_ID is not a title, skip in this branch
            # To be safe, skip if channel_title doesn't match target channel title
            # (You can change TARGET_CHANNEL_ID to a title if needed)
            # For now: skip because we cannot reliably match by title
            continue

        # Fetch metadata from SQLite
        c.execute("""
            SELECT title, channel_title, view_count, duration
            FROM videos
            WHERE id = ?
        """, (vid,))
        row = c.fetchone()
        if row:
            title, channel, view_count, duration = row
            results.append({
                "video_id": vid,
                "title": title,
                "channel_title": channel,
                "view_count": view_count,
                "duration": duration,
                "distance": float(dist)
            })

        # stop when we have enough final results
        if len(results) >= k:
            break

    return results

if __name__ == "__main__":
    q = input("Enter your search query: ").strip()
    if not q:
        print("Empty query, exiting.")
    else:
        res = search(q, k=5)
        if not res:
            print("No matching videos found for the target channel.")
        else:
            print("\nTop results (filtered by channel_id):\n")
            for i, r in enumerate(res, 1):
                print(f"{i}. {r['title']}  ({r['channel_title']})")
                print(f"   video_id: {r['video_id']}")
                print(f"   views: {r['view_count']} | duration: {r['duration']} seconds")
                print(f"   similarity score: {r['distance']:.4f}\n")
