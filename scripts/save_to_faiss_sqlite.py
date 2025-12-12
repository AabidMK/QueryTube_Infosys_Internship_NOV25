# save_to_faiss_sqlite.py
"""
Save embeddings + metadata to FAISS (vectors) and SQLite (metadata).
Outputs:
 - videos.index    (FAISS index file)
 - meta.db         (SQLite metadata DB)
 - ids.json        (list mapping index -> video_id)
"""
import os
import json
import sqlite3
import pandas as pd
import numpy as np
import faiss
import sys

CSV = "final_output_with_embeddings.csv"
INDEX_FILE = "videos.index"
META_DB = "meta.db"
IDS_FILE = "ids.json"

def safe_int(x):
    try:
        if x is None or x == "" or (isinstance(x, float) and np.isnan(x)) or str(x).lower() == "nan":
            return 0
        return int(float(x))
    except:
        return 0

def load_embedding(e):
    if pd.isna(e):
        return None
    if isinstance(e, str):
        try:
            return np.array(json.loads(e), dtype="float32")
        except Exception:
            # maybe a stringified python list; try eval fallback (risky but helpful)
            try:
                return np.array(eval(e), dtype="float32")
            except Exception:
                return None
    # if it's already list/ndarray
    try:
        return np.array(e, dtype="float32")
    except Exception:
        return None

def main():
    if not os.path.exists(CSV):
        print(f"ERROR: Input CSV not found: {CSV}", file=sys.stderr)
        sys.exit(1)

    df = pd.read_csv(CSV)
    total = len(df)
    print(f"Loaded {total} rows from {CSV}")

    vectors = []
    ids = []
    metas = []

    # iterate and build lists, skipping rows with bad/missing embeddings
    for i, row in df.iterrows():
        emb = load_embedding(row.get("embedding"))
        if emb is None:
            print(f"  [skip] row {i}: missing/invalid embedding")
            continue
        if emb.ndim != 1:
            print(f"  [skip] row {i}: embedding not 1D")
            continue

        vid = str(row.get("id") or row.get("video_id") or i)
        title = row.get("title", "") or ""
        transcript = row.get("transcript", "") or ""
        channel = row.get("channel_title", "") or ""

        view_count = safe_int(row.get("view_count", row.get("viewCount", 0)))
        duration = safe_int(row.get("duration_seconds", row.get("duration", 0)))

        vectors.append(emb.astype("float32"))
        ids.append(vid)
        metas.append({
            "video_id": vid,
            "title": title,
            "transcript": transcript,
            "channel_title": channel,
            "view_count": view_count,
            "duration": duration
        })

    if len(vectors) == 0:
        print("No valid embeddings found. Exiting.", file=sys.stderr)
        sys.exit(1)

    vectors = np.vstack(vectors).astype("float32")
    d = vectors.shape[1]
    print(f"Prepared {vectors.shape[0]} vectors with dimension {d}")

    # Build FAISS index (L2)
    index = faiss.IndexFlatL2(d)
    index.add(vectors)
    faiss.write_index(index, INDEX_FILE)
    print(f"FAISS index saved to {INDEX_FILE}")

    # Save ids mapping
    with open(IDS_FILE, "w", encoding="utf-8") as f:
        json.dump(ids, f, ensure_ascii=False)
    print(f"IDs mapping saved to {IDS_FILE}")

    # Save metadata to SQLite
    if os.path.exists(META_DB):
        os.remove(META_DB)
    conn = sqlite3.connect(META_DB)
    c = conn.cursor()
    c.execute('''
    CREATE TABLE videos (
        id TEXT PRIMARY KEY,
        title TEXT,
        transcript TEXT,
        channel_title TEXT,
        view_count INTEGER,
        duration INTEGER
    )
    ''')
    rows = [(m["video_id"], m["title"], m["transcript"], m["channel_title"], m["view_count"], m["duration"]) for m in metas]
    c.executemany('INSERT INTO videos VALUES (?,?,?,?,?,?)', rows)
    conn.commit()
    conn.close()
    print(f"Metadata saved to {META_DB}")

    print("All done. You can run the search script to query the index.")

if __name__ == "__main__":
    main()
