import numpy as np
from sentence_transformers import SentenceTransformer
from ..vectordb.setup_chroma import get_collection


class VideoSearchEngine:
    """
    Handles:
    1. Converting user query -> embeddings
    2. Performing similarity search in ChromaDB
    3. Returning top results with full metadata
    """

    def __init__(self):
        print("Loading search model...")
        self.model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
        self.collection = get_collection()
        print("Search engine ready!")

    def search(self, query, top_k=5):
        if not query or not isinstance(query, str) or query.strip() == "":
            raise ValueError("Query must be a valid non-empty string")

        query_embedding = self.model.encode(query)

        results = self.collection.query(
            query_embeddings=[query_embedding],
            n_results=top_k
        )

        output = []

        for i in range(len(results["documents"][0])):
            meta = results["metadatas"][0][i]

            item = {
                "video_id": results["metadatas"][0][i]["video_id"],
                "title": results["metadatas"][0][i]["title"],
                "channel": results["metadatas"][0][i]["channel"],
                "thumbnail": results["metadatas"][0][i]["thumbnail"],
                "views": results["metadatas"][0][i]["views"],
                "likes": results["metadatas"][0][i]["likes"],
                "duration": results["metadatas"][0][i]["duration_readable"],
                "similarity_score": float(results["distances"][0][i])
            }

            output.append(item)

        return output


# ---------------- TESTING -----------------
if __name__ == "__main__":
    engine = VideoSearchEngine()

    query = input("Enter your search query: ")
    results = engine.search(query)

    print("\nTop Results:\n")
    for r in results:
        print(r)
