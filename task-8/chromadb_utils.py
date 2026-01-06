import chromadb
from sentence_transformers import SentenceTransformer
from google import genai
import csv
import os
from dotenv import load_dotenv
import pandas as pd
import ast


# ==========================================================
# Environment + Models
# ==========================================================
load_dotenv()

client = genai.Client(api_key=os.getenv("GOOGLE_API_KEY"))
MODEL_NAME = "gemini-flash-latest"  

embedder = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")


# ==========================================================
# ChromaDB
# ==========================================================
client_chroma = chromadb.PersistentClient(path="mydb")
collection = client_chroma.get_or_create_collection("semantic_search_tube")

# ==========================================================
# SAFE UTILITIES
# ==========================================================
def safe_int(v):
    try:
        return int(float(v))
    except Exception:
        return 0


# ==========================================================
# INGESTION
# ==========================================================
def ingest_csv(csv_path: str) -> int:
    df = pd.read_csv(csv_path)

    required_cols = [
        "video_id",
        "chunk_text",
        "chunk_index",
        "title",
        "channel_title",
        "view_count",
        "like_count",
        "duration_seconds"
    ]

    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing column: {col}")

    ids = df.apply(lambda row: f"{row['video_id']}_{row['chunk_index']}", axis=1).tolist()
    documents = df["chunk_text"].fillna("").astype(str).tolist()
    embeddings = df["chunk_text"].apply(lambda x: embedder.encode(x).tolist()).tolist()
    # Build video_id -> channel_name map (first valid value wins)
    video_channel_map = {}

    for _, row in df.iterrows():
     if row["video_id"] not in video_channel_map:
        val = row["channel_title"]
        if pd.notna(val) and str(val).strip():
            video_channel_map[row["video_id"]] = str(val).strip()


    metadatas = [
        {
            "video_id": row["video_id"],
            "chunk_index": safe_int(row["chunk_index"]),
            "title": row["title"],
            "channel_name": (
                str(row["channel_title"]).strip()
                if pd.notna(row["channel_title"]) and str(row["channel_title"]).strip()
                else None
            ),
            "view_count": safe_int(row["view_count"]),
            "like_count": safe_int(row["like_count"]),
            "duration_seconds": safe_int(row["duration_seconds"]),
            "published_at": row["published_at"]
        }
        for _, row in df.iterrows()
    ]

    collection.upsert(
        ids=ids,
        documents=documents,
        embeddings=embeddings,
        metadatas=metadatas
    )

    return len(ids)

# ==========================================================
# SEARCH
# ==========================================================
def search(query: str, top_k: int = 20):
    query_embedding = embedder.encode(query).tolist()

    results = collection.query(
        query_embeddings=[query_embedding],
        n_results=top_k,
        include=["documents", "metadatas", "distances"]
    )

    if not results or not results.get("documents") or not results["documents"][0]:
        return {"results": []}

    output = []
    for doc, meta, dist in zip(
        results["documents"][0],
        results["metadatas"][0],
        results["distances"][0]
    ):
        output.append({
            "video_id": meta.get("video_id"),
            "title": meta.get("title"),
            "channel_name": meta.get("channel_name"),       
            "similarity": round(1 - dist, 4),
            "text": (doc[:200] + "...") if doc else ""
        })

    return {"results": output}


# ==========================================================
# SUMMARIZE full video transcript
# ==========================================================


def summarize_video(video_id: str):
    # ---- fetch transcript chunks ----
    results = collection.get(
        where={"video_id": video_id},
        include=["documents"]
    )

    documents = results.get("documents", [])
    if not documents:
        return {"error": f"No transcript found for video_id: {video_id}"}

    # ---- combine into full transcript ----
    full_transcript = " ".join(documents)

    # ---- your prompt exactly as given ----
    prompt = f"""
Summarize the following YouTube video transcript.
Focus on key ideas and takeaways.

Transcript:
{full_transcript}
"""

    # ---- summarization using new google.genai ----
    response = client.models.generate_content(
        model="gemini-flash-latest",   # replacement for gemini-flash-latest
        contents=prompt
    )

    return {
        "video_id": video_id,
        "summary": response.text,
        "transcript_length": len(full_transcript)
    }
