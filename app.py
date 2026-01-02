from fastapi import FastAPI, UploadFile, File
from pydantic import BaseModel
import chromadb
import pandas as pd
import ast
import os
import re
from google import genai
from fastapi import HTTPException

from fastapi.responses import RedirectResponse
from sentence_transformers import SentenceTransformer

embed_model = SentenceTransformer("all-MiniLM-L6-v2")


# -----------------------
# App Init
# -----------------------
app = FastAPI(title="VectorDB API")
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# -----------------------
# ChromaDB
# -----------------------
client = chromadb.PersistentClient(path="./chroma_db")
collection = client.get_or_create_collection(name="youtube_videos")

# -----------------------
# Schemas
# -----------------------
class SearchQuery(BaseModel):
    query: str
    top_k: int = 5



class VideoIDRequest(BaseModel):
    video_id: str


# -----------------------
# Root
# -----------------------
@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/docs")



# -----------------------
# CSV INGESTION
# -----------------------
@app.post("/ingest-csv")
async def ingest_csv(file: UploadFile = File(...)):

    df = pd.read_csv(file.file)

    for _, row in df.iterrows():
        video_id = str(row["id"])

        # Skip duplicates
        existing = collection.get(ids=[video_id])
        if existing["ids"]:
            continue

        embedding = ast.literal_eval(row["embedding"])

        collection.add(
            ids=[video_id],
            embeddings=[embedding],
            documents=[row["transcript"]],
            metadatas=[{
                "video_id": row["video_id"],
                "title": row["title"],
                "channel_title": row["channel_title"],
                "view_count": int(row["view_count"]),
                "duration_seconds": int(row["duration_seconds"])
            }]
        )

    return {"status": "success", "message": "CSV ingested successfully"}


# -----------------------
# SEARCH (BY EMBEDDING)
# -----------------------
@app.post("/search")
def search_videos(data: SearchQuery):
    query_embedding = embed_model.encode(data.query).tolist()

    results = collection.query(
        query_embeddings=[query_embedding],
        n_results=data.top_k
    )

    response = []

    if not results["ids"] or not results["ids"][0]:
        return response

    for i in range(len(results["ids"][0])):
        meta = results["metadatas"][0][i]

        response.append({
            "video_id": results["ids"][0][i],
            "title": meta.get("title"),
            "channel": meta.get("channel_title"),
            "view_count": meta.get("view_count"),
            "duration_seconds": meta.get("duration_seconds"),
            "similarity_score": results["distances"][0][i],
        })

    return response




# -----------------------
# SUMMARIZATION
# -----------------------

client = genai.Client(api_key="api_key")

@app.post("/summarize")
async def summarize_video(data: VideoIDRequest):

    result = collection.get(
        ids=[data.video_id],
        include=["documents", "metadatas"]
    )

    if not result["documents"]:
        raise HTTPException(status_code=404, detail="Video not found")

    transcript = result["documents"][0]
    title = result["metadatas"][0]["title"]

    # CLEAN PROMPT (no bullets, no intro text)
    prompt = f"""
Summarize the following YouTube video transcript in a clear paragraph format.
Do NOT use bullet points.
Do NOT add introductory phrases.
Start directly with the summary content.

Title: {title}

Transcript:
{transcript}
"""

    try:
        response = client.models.generate_content(
            model="gemini-2.5-flash-lite",
            contents=prompt
        )

        raw_summary = response.text.strip()

        # -------- CLEAN OUTPUT --------
        # Remove bullets, markdown, extra newlines
        clean_summary = raw_summary
        clean_summary = re.sub(r"[*•]+", "", clean_summary)
        clean_summary = re.sub(r"\n+", " ", clean_summary)
        clean_summary = re.sub(r"\s{2,}", " ", clean_summary)
        clean_summary = clean_summary.strip()

    except Exception as e:
        clean_summary = transcript[:500] + "..."

    return {
        "video_id": data.video_id,
        "title": title,
        "summary": clean_summary
    }

