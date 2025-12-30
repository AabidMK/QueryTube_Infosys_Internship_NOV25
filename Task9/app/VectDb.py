import chromadb
from chromadb import PersistentClient
# for loading the csv file
import pandas as pd
#for converting python object looking string to python
import ast
def ingest(df):
    try:
        client = chromadb.PersistentClient(path="chroma_db")
        collec = client.get_or_create_collection(name="QueryTube")

        embeds = df["final_embedding"].apply(ast.literal_eval).tolist()
        ids = df["id"].astype(str).tolist()
        documents = df["transcript"].tolist()

        metadatas = [
            {
                "video_id": row["id"],
                "title": row["title"],
                "channel_title": row["channel_title"],
                "view_count": row["viewCount"],
                "duration": row["duration"],
            }
            for _, row in df.iterrows()
        ]

        collec.add(
            ids=ids,
            embeddings=embeds,
            documents=documents,
            metadatas=metadatas,
        )

        result = collec.get(ids=ids)

        if len(result["ids"]) == len(ids):
            return {
                "status": "success",
                "rows_ingested": len(ids),
            }

        return {
            "status": "failed",
            "message": "Vectors not retrievable after insert",
        }

    except Exception as e:
        return {"status": "failed", "error": str(e)}
