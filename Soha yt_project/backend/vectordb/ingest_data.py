import pandas as pd
from sentence_transformers import SentenceTransformer
from ..preprocessing.chunking import chunk_text
from .setup_chroma import get_collection


def safe_str(value, fallback="unknown"):
    if pd.isna(value) or value is None:
        return fallback
    return str(value)


def safe_int(value, fallback=0):
    try:
        return int(value)
    except:
        return fallback


def load_dataset(csv_path: str):
    df = pd.read_csv(csv_path)
    print(f"Dataset loaded successfully. Rows: {len(df)}")
    return df


def get_embedding_model():
    print("Loading embedding model...")
    model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
    print("Embedding model loaded!")
    return model


def ingest_data(csv_path: str):
    df = load_dataset(csv_path)
    model = get_embedding_model()
    collection = get_collection()

    documents = []
    metadatas = []
    ids = []

    print("\nStarting ingestion...\n")

    for _, row in df.iterrows():

        video_id = safe_str(row.get("id"))
        title = safe_str(row.get("title"))
        channel = safe_str(row.get("channel_title"))
        thumbnail = safe_str(row.get("thumbnail_high"), "")
        views = safe_int(row.get("viewCount"))
        likes = safe_int(row.get("likeCount"))
        duration_seconds = safe_int(row.get("duration_seconds"))
        duration_readable = safe_str(row.get("duration"))

        transcript = safe_str(row.get("transcript"), "")

        if transcript.strip() == "" or transcript.lower() == "nan":
            continue

        chunks = chunk_text(transcript, chunk_size=400)

        for i, chunk in enumerate(chunks):
            model.encode(chunk)

            documents.append(chunk)
            ids.append(f"{video_id}_chunk_{i}")

            metadatas.append({
                "video_id": video_id,
                "title": title,
                "channel": channel,
                "thumbnail": thumbnail,
                "views": views,
                "likes": likes,
                "duration_seconds": duration_seconds,
                "duration_readable": duration_readable
            })

    print(f"Total chunks stored: {len(documents)}")

    collection.add(
        documents=documents,
        metadatas=metadatas,
        ids=ids
    )

    print("\nIngestion completed successfully 🚀")
