from fastapi import FastAPI
from pydantic import BaseModel
from sentence_transformers import SentenceTransformer
import chromadb

# App init
app = FastAPI(title="VectorDB API")

# Embedding model
embed_model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

# ChromaDB
client = chromadb.PersistentClient(path="./chroma_db")
collection = client.get_or_create_collection(name="youtube_videos")

# Schemas
class VideoData(BaseModel):
    video_id: str
    title: str
    transcript: str
    channel_title: str
    view_count: int
    duration_seconds: int


class VideoIDRequest(BaseModel):
    video_id: str


# Root
@app.get("/")
def root():
    return {"message": "VectorDB API is running 🚀"}


# Ingest Endpoint
@app.post("/ingest")
def ingest_video(data: VideoData):

    # Check duplicate
    existing = collection.get(ids=[data.video_id])
    if existing["ids"]:
        return {
            "status": "skipped",
            "message": f"Video {data.video_id} already exists"
        }

    combined_text = data.title + " " + data.transcript
    embedding = embed_model.encode(combined_text).tolist()

    collection.add(
        ids=[data.video_id],
        embeddings=[embedding],
        documents=[data.transcript],
        metadatas=[{
            "title": data.title,
            "channel_title": data.channel_title,
            "view_count": data.view_count,
            "duration_seconds": data.duration_seconds
        }]
    )

    return {
        "status": "success",
        "message": f"Video {data.video_id} ingested successfully"
    }


# Search by Video ID
@app.post("/search")
def search_by_video_id(data: VideoIDRequest):

    result = collection.get(
        ids=[data.video_id],
        include=["documents", "metadatas"]
    )

    if not result["documents"]:
        return {"error": "Video not found"}

    return {
        "video_id": data.video_id,
        "title": result["metadatas"][0]["title"],
        "channel": result["metadatas"][0]["channel_title"],
        "view_count": result["metadatas"][0]["view_count"],
        "duration_seconds": result["metadatas"][0]["duration_seconds"]
    }


# Summarize using Transcript
@app.post("/summarize")
def summarize_video(data: VideoIDRequest):

    result = collection.get(
        ids=[data.video_id],
        include=["documents", "metadatas"]
    )

    if not result["documents"]:
        return {"error": "Video not found"}

    transcript = result["documents"][0]
    title = result["metadatas"][0]["title"]

    # Simple extractive summary (first ~600 chars)
    summary = transcript[:600] + "..."

    return {
        "video_id": data.video_id,
        "title": title,
        "summary": summary
    }
