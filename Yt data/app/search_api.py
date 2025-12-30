from flask import Blueprint, request, jsonify
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient

search_bp = Blueprint("search", __name__)

COLLECTION_NAME = "youtube_videos"
DB_PATH = "local_qdrant_db"

model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

@search_bp.route("/search", methods=["GET"])
def search_videos():
    print("🔍 /search endpoint hit")

    query = request.args.get("query")
    if not query:
        return jsonify({"error": "query parameter is required"}), 400

    # ✅ Create client ONLY when request comes
    client = QdrantClient(path=DB_PATH)

    query_embedding = model.encode(query).tolist()

    results = client.query_points(
        collection_name=COLLECTION_NAME,
        query=query_embedding,
        limit=5
    ).points

    client.close()  # ✅ release lock

    response = []
    for r in results:
        response.append({
            "video_id": r.payload.get("video_id"),
            "title": r.payload.get("title"),
            "channel_name": r.payload.get("channel_title"),
            "similarity": round(r.score, 4)
        })

    return jsonify(response)
