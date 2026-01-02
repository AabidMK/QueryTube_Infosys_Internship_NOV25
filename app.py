from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sentence_transformers import SentenceTransformer
import chromadb
from transformers import pipeline
import pandas as pd
import io

app = FastAPI()

# ============================
# ✅ CORS
# ============================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================
# ✅ ChromaDB
# ============================
chroma_client = chromadb.PersistentClient(path="./chroma_db")
collection = chroma_client.get_or_create_collection(name="video_collection")

# ============================
# ✅ Models
# ============================
model = SentenceTransformer("all-MiniLM-L6-v2")
summarizer = pipeline("summarization", model="facebook/bart-large-cnn")

# ============================
# ✅ Schemas
# ============================
class SearchRequest(BaseModel):
    query: str
    n_results: int = 5

class SummarizeRequest(BaseModel):
    video_id: str

# ============================
# ✅ Ingest Endpoint (CSV Upload)
# ============================
@app.post("/ingest")
async def ingest_csv(file: UploadFile = File(...)):
    """
    Expects columns from your CSV:
    id, title, transcript, channel_title, viewcount, duration_seconds, embedding
    """
    try:
        contents = await file.read()
        df = pd.read_csv(io.BytesIO(contents))

        required_cols = {"id", "title", "transcript", "channel_title", "viewcount", "duration_seconds", "embedding"}
        missing = required_cols - set(df.columns)
        if missing:
            return {"status": "error", "message": f"Missing columns: {sorted(list(missing))}"}

        rows = 0
        for _, row in df.iterrows():
            transcript = str(row["transcript"])
            if not transcript or transcript.strip().lower() in {"nan", ""}:
                continue

            # Use pre-computed embedding from CSV
            embedding = eval(row["embedding"])  # convert string → list of floats

            # Safe conversion for viewcount and duration
            try:
                view_count = int(float(row["viewcount"]))
            except Exception:
                view_count = 0

            try:
                duration = float(row["duration_seconds"])
            except Exception:
                duration = 0.0

            collection.add(
                ids=[str(row["id"])],
                documents=[transcript],
                embeddings=[embedding],
                metadatas=[{
                    "title": str(row["title"]),
                    "channel_title": str(row["channel_title"]),
                    "view_count": view_count,
                    "duration": duration,
                }]
            )
            rows += 1

        return {"status": "success", "rows_ingested": rows}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# ============================
# ✅ Search Endpoint
# ============================
@app.post("/search")
def search_collection(request: SearchRequest):
    try:
        embedding = model.encode(request.query).tolist()
        results = collection.query(
            query_embeddings=[embedding],
            n_results=request.n_results
        )
        formatted = {
            "ids": results.get("ids", []),
            "documents": results.get("documents", []),
            "metadatas": results.get("metadatas", []),
            "distances": results.get("distances", [])
        }
        return {"status": "success", "results": formatted}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# ============================
# ✅ Summarize Endpoint
# ============================
@app.post("/summarize")
def summarize_transcript(request: SummarizeRequest):
    try:
        results = collection.get(ids=[request.video_id])
        docs = results.get("documents", [])
        if not docs or not docs[0] or docs[0].strip().lower() in {"nan", ""}:
            return {"video_id": request.video_id, "summary": "No valid transcript found for summarization."}

        transcript = docs[0]
        words = transcript.split()
        if len(words) > 800:
            transcript = " ".join(words[:800])

        summary = summarizer(
            transcript,
            max_length=120,
            min_length=40,
            do_sample=False
        )[0]["summary_text"]

        return {
            "video_id": request.video_id,
            "summary": summary
        }
    except Exception:
        return {
            "video_id": request.video_id,
            "summary": "Error occurred while summarizing."
        }

# # uvicorn app:app --reload

