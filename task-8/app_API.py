from fastapi import FastAPI, UploadFile, File, Form ,Query
import shutil
from fastapi.middleware.cors import CORSMiddleware
from chromadb_utils import ingest_csv, search, summarize_video


app = FastAPI()

# --- CORS middleware ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # later we can restrict to ["http://localhost:3000"]
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def root():
    return {"message": "Semantic AI Search Tube API is running!"}

# Ingest by CSV path (Typing the path)
@app.post("/ingest-path")
def ingest_by_path(csv_path: str = Query(..., description="Path to CSV file")):
    rows = ingest_csv(csv_path)
    return {"message": "Path ingestion successful", "rows": rows}

# Ingest by CSV upload (Browse & upload file)
@app.post("/ingest-upload")
def ingest_upload(file: UploadFile = File(...)):
    temp_path = f"uploaded_{file.filename}"
    with open(temp_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    rows = ingest_csv(temp_path)
    return {"message": "File uploaded & ingested", "rows": rows}


# SEARCH
@app.get("/search")
def search_endpoint(q: str):
    return search(q)

# SUMMARIZE
@app.get("/summarize/{video_id}")
def summarize_endpoint(video_id: str):
    return summarize_video(video_id)
