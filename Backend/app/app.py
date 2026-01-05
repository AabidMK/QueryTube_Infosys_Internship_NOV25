from fastapi import FastAPI, UploadFile, File, Query
import pandas as pd
import ast
import os
from chromadb import PersistentClient
from sentence_transformers import SentenceTransformer
from google import genai
from dotenv import load_dotenv
from .Summary import Summary
from .Fetch import fetch
from .VectDb import ingest

# --------------------------------
# FASTAPI INIT
# --------------------------------
app = FastAPI(title="QueryTube API")




# --------------------------------
# CSV INGESTION
# --------------------------------
@app.post("/ingest-csv")
async def ingest_csv(file: UploadFile = File(...)):
  
    df = pd.read_csv(file.file)
    response = ingest(df)
    return response


# SEMANTIC SEARCH
@app.get("/search")
def search(
    query: str = Query(..., description="Search query"),
    top_k: int = Query(6, ge=1, le=20),):
   
    response= fetch(query=query,top_k=top_k)

    
    return {
        "query": query,
        "results": response,
    }



# TRANSCRIPT SUMMARY

@app.get("/summary")
def summary(video_id: str):
    text = Summary(video_id)
    
    if not text:
        return {
            "status": "not_found",
            "message": "Video ID not found",
        }

    return {
        "status": "success",
        "video_id": video_id,
        "summary": text,
    }

from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",  # Vite
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

