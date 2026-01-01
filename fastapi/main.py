from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
import shutil
import os
from ingest import ingest_csv
from search import semantic_search
from summarize import summarize_video

app = FastAPI(title="QueryTube API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def greet():
    return "Welcome to QueryTube AI"

# ---------------------------
# INGEST ENDPOINT
# ---------------------------
@app.post("/ingest")
async def ingest(file: UploadFile = File(...)):
    temp_path = f"temp_{file.filename}"

    with open(temp_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    try:
        rows = ingest_csv(temp_path)
    finally:
        os.remove(temp_path)

    return {"status": "success", "rows_ingested": rows}

# ---------------------------
# SEMANTIC SEARCH ENDPOINT
# ---------------------------
@app.get("/search")
def search(query: str):
    return semantic_search(query)

# ---------------------------
# SUMMARY ENDPOINT
# ---------------------------
@app.get("/summary/{video_id}")
def summary(video_id: str):
    return {
        "video_id": video_id,
        "summary": summarize_video(video_id)
    }
