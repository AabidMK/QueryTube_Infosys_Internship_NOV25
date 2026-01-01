import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from .embedding_model import EmbeddingModel   # <--- FIXED


def print_similarity(a, b, model):
    emb1 = model.get_embedding(a)
    emb2 = model.get_embedding(b)

    score = cosine_similarity([emb1], [emb2])[0][0]
    print(f"Similarity between '{a}' and '{b}' = {round(float(score), 3)}")


if __name__ == "__main__":
    model = EmbeddingModel()

    print("\n--- Checking Similarity ---\n")

    # Similar meaning words
    print_similarity("india", "country", model)
    print_similarity("football", "sports", model)
    print_similarity("teacher", "education", model)

    # Dissimilar words
    print_similarity("cat", "rocket", model)
    print_similarity("apple", "war", model)
