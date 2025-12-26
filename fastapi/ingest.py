import chromadb
import pandas as pd
import ast

CHROMA_PATH = "QueryTube_db"
COLLECTION_NAME = "youtube_videos_metadata"

def ingest_csv(csv_path:str) -> int:
    df = pd.read_csv(csv_path)

    required_cols = [
        "id",
        "transcript",
        "title_transcript_embedding",
        "title",
        "channel_title",
        "viewCount",
        "duration_seconds"
    ]

    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing column: {col}")

    client = chromadb.PersistentClient(path=CHROMA_PATH)
    collection = client.get_or_create_collection(name=COLLECTION_NAME,metadata={"hnsw:space": "cosine"})

    ids = df["id"].astype(str).tolist()
    documents = df["transcript"].fillna("").astype(str).tolist()

    embeddings = df["title_transcript_embedding"].apply(
        lambda x: ast.literal_eval(x)
    ).tolist()

    def safe_int(v):
        return 0 if pd.isna(v) else int(v)

    metadatas = [
        {
            "title": row["title"],
            "channel_title": row["channel_title"],
            "view_count": safe_int(row["viewCount"]),
            "duration_seconds": safe_int(row["duration_seconds"])
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

if __name__ == "__main__":
    csv_path = input("Enter path to CSV file: ").strip()
    rows = ingest_csv(csv_path)
    print(f"\n✅ Ingested {rows} rows into ChromaDB")
    


