from fastapi import FastAPI
from pydantic import BaseModel
from sentence_transformers import SentenceTransformer
import chromadb

# Initialize FastAPI
app = FastAPI()

# ✅ New (supported)
chroma_client = chromadb.PersistentClient(path="./chroma_db")
collection = chroma_client.get_or_create_collection(name="video_collection")


# Load embedding model
model = SentenceTransformer("all-MiniLM-L6-v2")

class VideoData(BaseModel):
    id: str
    transcript: str
    title: str
    channel_title: str
    view_count: int
    duration: float

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
    
@app.get("/peek")
def peek_collection():
    try:
        raw = collection.peek()
        # Manually format the response
        formatted = {
            "ids": raw["ids"],
            "documents": raw["documents"],
            "metadatas": raw["metadatas"],
            "embedding_dims": [len(vec) for vec in raw["embeddings"]]
        }
        return {"status": "success", "results": formatted}
    except Exception as e:
        return {"status": "error", "message": str(e)}
