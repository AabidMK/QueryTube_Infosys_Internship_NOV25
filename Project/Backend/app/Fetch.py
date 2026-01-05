import pandas as pd
import chromadb
from sentence_transformers import SentenceTransformer
model = SentenceTransformer('intfloat/multilingual-e5-large')

#i think this persistantclient creates or load the dataset without creating it itself
client = chromadb.PersistentClient(path='../Task6/chroma_db')
#z = input("enter the query:")

#collection name is like table name so should be equal to already 
#existed collection
#print(client.list_collections())
collection = client.get_collection(name='QueryTube')
def fetch(query,top_k):
    query_embedding = model.encode(
        f"query: {query}",
        normalize_embeddings=True
    ).tolist()

    results = collection.query(
        query_embeddings=[query_embedding],
        n_results=top_k,
        include=["metadatas", "distances"]
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
                "view_count": meta["view_count"],
                "likes":meta['likes'],
                "Upload_date":meta["Upload_date"],
                "thumbnail":meta['thumbnail'],
                "duration": meta["duration"],
                "similarity": similarity,
            }
        )
    return response
    