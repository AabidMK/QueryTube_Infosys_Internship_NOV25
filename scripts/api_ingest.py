from fastapi import FastAPI
from pydantic import BaseModel
import faiss
import os
import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer

# --------------------
# App
# --------------------
app = FastAPI(title="VectorDB Ingestion API")

# --------------------
# Paths
# --------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
VECTOR_PATH = os.path.join(BASE_DIR, "vector_db", "videos.index")
META_PATH = os.path.join(BASE_DIR, "vector_db", "meta_for_index.csv")

# --------------------
# Load model
# --------------------
model = SentenceTransformer("all-MiniLM-L6-v2")
DIM = 384

# --------------------
# Load / init FAISS
# --------------------
if os.path.exists(VECTOR_PATH):
    index = faiss.read_index(VECTOR_PATH)
    meta_df = pd.read_csv(META_PATH)
else:
    index = faiss.IndexFlatL2(DIM)
    meta_df = pd.DataFrame(columns=["video_id", "title", "channel_title"])

# --------------------
# Request schema
# --------------------
class VideoData(BaseModel):
    video_id: str
    title: str
    channel_title: str
    transcript: str

# --------------------
# Ingest endpoint
# --------------------
@app.post("/ingest")
def ingest_video(data: VideoData):
    embedding = model.encode(data.transcript).astype("float32")
    index.add(np.array([embedding]))

    meta_df.loc[len(meta_df)] = [
        data.video_id,
        data.title,
        data.channel_title
    ]

    faiss.write_index(index, VECTOR_PATH)
    meta_df.to_csv(META_PATH, index=False)

    return {
        "status": "success",
        "message": "Video ingested into vector DB",
        "total_vectors": index.ntotal
    }
