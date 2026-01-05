from fastapi import FastAPI, UploadFile, File, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import pandas as pd
import numpy as np
import json
import faiss
import os
import subprocess

# ===============================
# App Init
# ===============================
app = FastAPI(title="QueryTube Backend API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # allow frontend
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ===============================
# Paths
# ===============================
VECTOR_DIR = "vector_db"
INDEX_PATH = os.path.join(VECTOR_DIR, "videos.index")
META_PATH = os.path.join(VECTOR_DIR, "meta_for_index.csv")

os.makedirs(VECTOR_DIR, exist_ok=True)

EMBED_DIM = 384

# ===============================
# Load / Init FAISS
# ===============================
if os.path.exists(INDEX_PATH):
    index = faiss.read_index(INDEX_PATH)
else:
    index = faiss.IndexFlatL2(EMBED_DIM)

# ===============================
# Models
# ===============================
class SearchRequest(BaseModel):
    query: str
    top_k: int = 5

# ===============================
# Health
# ===============================
@app.get("/")
def root():
    return {"status": "API running"}

# =========================================================
# 1️⃣ INGEST CSV (WITH EMBEDDINGS)
# =========================================================
@app.post("/ingest")
def ingest_csv(file: UploadFile = File(...)):

    df = pd.read_csv(file.file)

    required_cols = {"id", "title", "channel_title", "embedding", "combined_text"}
    if not required_cols.issubset(df.columns):
        return {"error": f"CSV must contain columns: {required_cols}"}

    # Parse embeddings
    vectors = np.vstack([
        np.array(json.loads(e), dtype="float32")
        for e in df["embedding"]
    ])

    index.add(vectors)
    faiss.write_index(index, INDEX_PATH)

    # Save metadata
    df[["id", "title", "channel_title", "combined_text"]].to_csv(
        META_PATH, index=False
    )

    return {
        "status": "success",
        "rows_ingested": len(df)
    }

# =========================================================
# 2️⃣ SEARCH
# =========================================================
@app.get("/search")
def search(query: str = Query(...), top_k: int = 5):

    if not os.path.exists(META_PATH) or index.ntotal == 0:
        return {"error": "Vector DB empty. Ingest first."}

    meta = pd.read_csv(META_PATH)

    from sentence_transformers import SentenceTransformer
    model = SentenceTransformer("all-MiniLM-L6-v2")

    q_vec = model.encode(query).astype("float32").reshape(1, -1)
    distances, indices = index.search(q_vec, top_k)

    results = []
    for rank, (idx, dist) in enumerate(zip(indices[0], distances[0]), start=1):
        row = meta.iloc[idx]
        results.append({
            "rank": rank,
            "video_id": row["id"],
            "title": row["title"],
            "channel_title": row["channel_title"],
            "similarity": round(1 / (1 + dist), 4)
        })

    return {
        "query": query,
        "results": results
    }

# =========================================================
# 3️⃣ SUMMARY (OLLAMA - PHI)
# =========================================================
@app.get("/summary")
def summarize(video_id: str):

    if not os.path.exists(META_PATH):
        return {"error": "No metadata found"}

    meta = pd.read_csv(META_PATH)
    row = meta[meta["id"] == video_id]

    if row.empty:
        return {"error": "Video ID not found"}

    transcript = row.iloc[0]["combined_text"]

    prompt = f"""
Summarize the following YouTube video transcript briefly.
Focus only on key ideas.

Transcript:
{transcript[:2000]}
"""

    result = subprocess.run(
        ["ollama", "run", "phi"],
        input=prompt.encode("utf-8"),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    summary = result.stdout.decode("utf-8", errors="ignore")

    return {
        "video_id": video_id,
        "title": row.iloc[0]["title"],
        "summary": summary.strip()
    }
