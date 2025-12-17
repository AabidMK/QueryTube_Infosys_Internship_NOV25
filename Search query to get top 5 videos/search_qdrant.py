from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer

QDRANT_URL = "http://localhost:6333"
COLLECTION_NAME = "youtube_videos"
TOP_K = 5

model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
client = QdrantClient(url=QDRANT_URL)

def search_videos(text: str, top_k: int = 5):
    # Convert query to embedding
    query_vector = model.encode(text).tolist()

    response = client.query_points(
        collection_name=COLLECTION_NAME,
        prefetch=[],
        query=query_vector,
        limit=top_k,
    )

    results = response.points

    output = []
    for point in results:
        payload = point.payload
        output.append({
            "video_id": payload.get("video_id"),
            "title": payload.get("title"),
            "channel_title": payload.get("channel_title"),
            "score": point.score
        })

    return output


if __name__ == "__main__":
    q = input("Enter your query: ")
    results = search_videos(q)

    for i, r in enumerate(results, 1):
        print(f"\nRank {i}")
        print(f"Video ID     : {r['video_id']}")
        print(f"Title        : {r['title']}")
        print(f"Channel Name : {r['channel_title']}")
        print(f"Similarity   : {r['score']:.4f}")
