import chromadb

def get_chroma_client():
    """
    Returns a persistent ChromaDB client.
    We store DB inside /data/chroma so it stays safe.
    """

    client = chromadb.PersistentClient(path="data/chroma")
    return client


def get_collection():
    """
    Creates / loads a collection where we will store
    our video transcript chunks + embeddings.
    """

    client = get_chroma_client()

    collection = client.get_or_create_collection(
        name="youtube_embeddings",
        metadata={"hnsw:space": "cosine"}  # similarity metric
    )

    return collection


# ----------- Test Only -------------
if __name__ == "__main__":
    col = get_collection()
    print("ChromaDB is ready!")
