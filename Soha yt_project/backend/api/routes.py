from fastapi import APIRouter, UploadFile, File
from pydantic import BaseModel
import shutil
import os

from backend.vectordb.ingest_data import ingest_data
from backend.search.search_service import VideoSearchEngine
from backend.summarizer.summary_service import summarize_video

router = APIRouter()

# ---------------- SEARCH ENGINE LOADED ONCE ----------------
search_engine = VideoSearchEngine()


# ---------------------- INGEST VIA PATH ----------------------
class IngestRequest(BaseModel):
    csv_path: str


@router.post("/ingest")
def ingest_from_path(request: IngestRequest):
    path = request.csv_path

    if not os.path.exists(path):
        return {
            "success": False,
            "message": "CSV path not found"
        }

    ingest_data(path)

    return {
        "success": True,
        "message": "Dataset ingested successfully from path"
    }


# ---------------------- INGEST VIA FILE UPLOAD ----------------------
@router.post("/ingest-upload")
def ingest_upload(file: UploadFile = File(...)):
    save_path = f"temp_{file.filename}"

    with open(save_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    ingest_data(save_path)

    os.remove(save_path)

    return {
        "success": True,
        "message": "File uploaded and ingested successfully"
    }


# ---------------------- SEARCH ----------------------
@router.get("/search")
def search(query: str, k: int = 5):
    results = search_engine.search(query, top_k=k)
    return {"results": results}


# ---------------------- SUMMARY ----------------------
@router.get("/summary")
def summary(video_id: str):
    result = summarize_video(video_id)
    return result
