from fastapi import FastAPI, UploadFile, File, Query
import pandas as pd
import ast
import os

from chromadb import PersistentClient
from sentence_transformers import SentenceTransformer
from google import genai
from dotenv import load_dotenv

# --------------------------------
# ENV + MODELS
# --------------------------------
load_dotenv('../Keys/.env')

GEMINI_KEY = os.getenv("Gemini_Key")
if not GEMINI_KEY:
    raise RuntimeError("Gemini_Key missing from .env")

Client = genai.Client(api_key=GEMINI_KEY)

embedding_model = SentenceTransformer(
    "intfloat/multilingual-e5-large"
)

# --------------------------------
# FASTAPI INIT
# --------------------------------
app = FastAPI(title="QueryTube API")

# --------------------------------
# CHROMADB
# --------------------------------
chroma_client = PersistentClient(path="../Task6/chroma_db")
collection = chroma_client.get_collection(name="QueryTube")

# --------------------------------
# HELPER: TRANSCRIPT SUMMARY
# --------------------------------
def get_transcript_by_video_id(video_id):
    result = collection.get(
        where={"video_id": video_id}
    )

    if not result.get("documents"):
        return None

    text = " ".join(result["documents"])

    response = Client.models.generate_content(
        model="gemini-2.5-flash",
        contents=f"Summarize this transcript in detail:\n{text}"
    )

    return response.text



# --------------------------------
# CSV INGESTION
# --------------------------------
@app.post("/ingest-csv")
async def ingest_csv(file: UploadFile = File(...)):
    df = pd.read_csv(file.file)

    df["final_embedding"] = df["final_embedding"].apply(
        ast.literal_eval
    )

    collection.add(
        ids=df["id"].astype(str).tolist(),
        embeddings=df["final_embedding"].tolist(),
        documents=df["transcript"].tolist(),
        metadatas=[
            {
                "video_id": row["id"],
                "title": row["title"],
                "channel_title": row["channel_title"],
                "view_count": row["viewCount"],
                "duration": row["duration_seconds"],
            }
            for _, row in df.iterrows()
        ],
    )

    return {
        "status": "success",
        "rows_ingested": len(df),
    }


# --------------------------------
# SEMANTIC SEARCH
# --------------------------------
@app.get("/search")
def search(
    query: str = Query(..., description="Search query"),
    top_k: int = Query(5, ge=1, le=20),
):
    query_embedding = embedding_model.encode(
        f"query: {query}",
        normalize_embeddings=True,
    ).tolist()

    results = collection.query(
        query_embeddings=[query_embedding],
        n_results=top_k,
        include=["metadatas", "distances"],
    )

    response = []
    for i, meta in enumerate(results["metadatas"][0]):
        similarity = round(
            1 - results["distances"][0][i], 4
        )

        response.append(
            {
                "rank": i + 1,
                "title": meta["title"],
                "channel": meta.get("channel_title"),
                "video_id": meta["video_id"],
                "similarity": similarity,
            }
        )

    return {
        "query": query,
        "results": response,
    }


# --------------------------------
# TRANSCRIPT SUMMARY
# --------------------------------
@app.get("/summary")
def summary(video_id: str):
    summary_text = get_transcript_by_video_id(video_id)

    if not summary_text:
        return {
            "status": "not_found",
            "message": "Video ID not found",
        }

    return {
        "status": "success",
        "video_id": video_id,
        "summary": summary_text,
    }
