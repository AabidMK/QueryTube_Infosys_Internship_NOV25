from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from upload_to_Qdrant import upload_csv_to_qdrant
from search_qdrant import search_videos
from video_summary import fetch_transcript, generate_gemini_content
import pandas as pd
import tempfile

app = FastAPI(title="Qdrant Vector Ingestion API")

@app.get("/")
def health():
    return{"status: API running"}

#=================== INGESTING CSV ===================
@app.post("/ingest/csv")
async def ingest_csv(file: UploadFile = File(...)):
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files allowed")
    
    try:
        #save uploaded file temporarily
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
            tmp.write(await file.read())
            tmp_path=tmp.name

        count = upload_csv_to_qdrant(tmp_path)

        return{
            "message": "Upload complete",
            "vector_inserted": count
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

#====================== SEARCH VIDEO ==================

@app.get("/search")

def search(query: str):
    return {
        "query" : query,
        "results" : search_videos(query)
    }

#====================== SUMMARRY ===================

@app.get("/video-summary/{video_id}")
def video_summary(video_id: str):
    transcript = fetch_transcript(video_id)

    if not transcript.strip():
        return {
            "video_id": video_id,
            "summary": None,
            "message": "No transcript found"
        }
    summary = generate_gemini_content(transcript)
    return {
        "video_id": video_id,
        "summary": summary
    }