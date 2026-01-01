from sentence_transformers import SentenceTransformer

class EmbeddingModel:
    """
    Simple helper class to load and reuse the embedding model.
    This avoids reloading model again and again.
    """

    def __init__(self, model_name="sentence-transformers/all-MiniLM-L6-v2"):
        print("\nLoading embedding model... please wait...")
        self.model = SentenceTransformer(model_name)
        print("Model loaded successfully!")

    def get_embedding(self, text):
        """
        Takes text and returns numerical vector (embedding).
        """
        return self.model.encode(text)
