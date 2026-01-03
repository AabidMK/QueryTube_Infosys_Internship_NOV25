from fastapi import FastAPI, UploadFile, File
from pydantic import BaseModel
import os
import pickle
import numpy as np
import faiss
import pandas as pd
from sentence_transformers import SentenceTransformer
from fastapi.middleware.cors import CORSMiddleware


# ===============================
# App Initialization
# ===============================
app = FastAPI(
    title="Infosys Task 1 - VectorDB API",
    description="API for CSV ingestion, search and video summarization",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# ===============================
# Paths & Config
# ===============================
METADATA_PATH = "metadata.pkl"
INDEX_PATH = "vector.index"
EMBEDDING_DIM = 384

EMBEDDING_MODEL = SentenceTransformer("all-MiniLM-L6-v2")

# Load or create FAISS index
if os.path.exists(INDEX_PATH):
    index = faiss.read_index(INDEX_PATH)
else:
    index = faiss.IndexFlatL2(EMBEDDING_DIM)

# ===============================
# Request Models
# ===============================
class SearchRequest(BaseModel):
    query: str
    top_k: int = 3

# ===============================
# Health Check
# ===============================
@app.get("/")
def health_check():
    return {"status": "API is running successfully"}

# ===============================
# CSV INGEST ENDPOINT
# ===============================
@app.post("/ingest-csv")
def ingest_csv(file: UploadFile = File(...)):
    """
    Upload CSV with columns:
    id, title, channel_title, viewCount, combined_text, duration_seconds
    """

    # Read CSV
    df = pd.read_csv(file.file)

    required_columns = {
        "id",
        "title",
        "channel_title",
        "viewCount",
        "combined_text",
        "duration_seconds",
    }

    if not required_columns.issubset(df.columns):
        return {
            "error": f"CSV must contain columns: {required_columns}"
        }

    # Load existing metadata
    if os.path.exists(METADATA_PATH):
        with open(METADATA_PATH, "rb") as f:
            metadata = pickle.load(f)
    else:
        metadata = []

    texts = df["combined_text"].fillna("").tolist()

    # Generate embeddings
    embeddings = EMBEDDING_MODEL.encode(texts, show_progress_bar=True)
    embeddings = np.array(embeddings).astype("float32")

    # Add to FAISS
    index.add(embeddings)
    faiss.write_index(index, INDEX_PATH)

    # Prepare metadata records
    for _, row in df.iterrows():
        metadata.append({
            "video_id": str(row["id"]),
            "title": row["title"],
            "channel_title": row["channel_title"],
            "view_count": int(row["viewCount"]),
            "duration": str(row["duration_seconds"]),
            "transcript": row["combined_text"],
        })

    # Save metadata
    with open(METADATA_PATH, "wb") as f:
        pickle.dump(metadata, f)

    return {
        "message": "CSV ingested successfully",
        "videos_added": len(df)
    }

# ===============================
# SEARCH ENDPOINT
# ===============================
@app.post("/search")
def search_videos(data: SearchRequest):

    if not os.path.exists(METADATA_PATH) or index.ntotal == 0:
        return {"error": "Vector database is empty"}

    with open(METADATA_PATH, "rb") as f:
        metadata = pickle.load(f)

    query_embedding = EMBEDDING_MODEL.encode(data.query)
    query_embedding = np.array([query_embedding]).astype("float32")

    distances, indices = index.search(query_embedding, data.top_k)

    results = []
    for idx, dist in zip(indices[0], distances[0]):
        if idx < len(metadata):
            item = metadata[idx].copy()
            item["similarity"] = float(dist)
            results.append(item)

    return {
        "query": data.query,
        "results": results
    }

# ===============================
# SUMMARIZE ENDPOINT
# ===============================
@app.post("/summarize")
def summarize_video(video_id: str):

    if not os.path.exists(METADATA_PATH):
        return {"error": "No data available"}

    with open(METADATA_PATH, "rb") as f:
        metadata = pickle.load(f)

    video = next((v for v in metadata if v["video_id"] == video_id), None)

    if not video:
        return {"error": "Video not found"}

    transcript = video.get("transcript", "")

    if not transcript:
        return {"error": "Transcript empty"}

    sentences = transcript.split(". ")
    summary = ". ".join(sentences[:5])

    return {
        "video_id": video_id,
        "title": video["title"],
        "summary": summary
    }
