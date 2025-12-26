from fastapi import FastAPI
from pydantic import BaseModel
from sentence_transformers import SentenceTransformer
import chromadb
from transformers import pipeline

# Initialize FastAPI
app = FastAPI()

# ✅ Persistent ChromaDB client
chroma_client = chromadb.PersistentClient(path="./chroma_db")
collection = chroma_client.get_or_create_collection(name="video_collection")

# Load embedding model
model = SentenceTransformer("all-MiniLM-L6-v2")

# Load summarizer once at startup
summarizer = pipeline("summarization", model="facebook/bart-large-cnn")

# -----------------------------
# Data Models
# -----------------------------
class VideoData(BaseModel):
    id: str
    transcript: str
    title: str
    channel_title: str
    view_count: int
    duration: float

class SearchRequest(BaseModel):
    query: str
    n_results: int = 3

class SummarizeRequest(BaseModel):
    video_id: str

# -----------------------------
# Endpoints
# -----------------------------

@app.post("/ingest")
def ingest_video(video: VideoData):
    try:
        embedding = model.encode(video.transcript).tolist()
        collection.add(
            ids=[video.id],
            documents=[video.transcript],
            embeddings=[embedding],
            metadatas=[{
                "title": video.title,
                "channel_title": video.channel_title,
                "view_count": video.view_count,
                "duration": video.duration,
            }]
        )
        return {"status": "success", "video_id": video.id}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/peek")
def peek_collection():
    try:
        raw = collection.peek()
        formatted = {
            "ids": raw["ids"],
            "documents": raw["documents"],
            "metadatas": raw["metadatas"],
            "embedding_dims": [len(vec) for vec in raw["embeddings"]]
        }
        return {"status": "success", "results": formatted}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/search")
def search_collection(request: SearchRequest):
    try:
        embedding = model.encode(request.query).tolist()
        results = collection.query(
            query_embeddings=[embedding],
            n_results=request.n_results
        )
        formatted = {
            "ids": results["ids"],
            "documents": results["documents"],
            "metadatas": results["metadatas"]
        }
        return {"status": "success", "results": formatted}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/summarize")
def summarize_transcript(request: SummarizeRequest):
    try:
        results = collection.get(ids=[request.video_id])
        transcript = results["documents"][0]

        summary = summarizer(
            transcript,
            max_length=60,
            min_length=20,
            do_sample=False
        )[0]["summary_text"]

        return {"status": "success", "video_id": request.video_id, "summary": summary}
    except Exception as e:
        return {"status": "error", "message": str(e)}
