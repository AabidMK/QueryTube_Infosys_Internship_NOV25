from sentence_transformers import SentenceTransformer
import numpy as np

# 1. Load a free, open-source embedding model
model_name = "sentence-transformers/all-MiniLM-L6-v2"
print(f"Loading model: {model_name}")
model = SentenceTransformer(model_name)

# 2. Some test "words" (you can also put short phrases)
texts = [
    "king",
    "queen",
    "man",
    "woman",
    "apple",
    "mango",
    "football",
    "cricket"
]

print("\nTexts:")
for i, t in enumerate(texts):
    print(f"{i}: {t}")

# 3. Generate embeddings (vectors)
embeddings = model.encode(texts, normalize_embeddings=True)  # shape: (len(texts), vector_dim)

print(f"\nEmbeddings shape: {embeddings.shape}")  # (n_texts, dim)

# 4. Define a cosine similarity function
def cosine_sim(a, b):
    return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))


# 5. Check similarity between some pairs
def show_similarity(i, j):
    sim = cosine_sim(embeddings[i], embeddings[j])
    print(f"sim('{texts[i]}', '{texts[j]}') = {sim:.4f}")

print("\nSimilarities between related words:")
show_similarity(0, 1)  # king - queen
show_similarity(2, 3)  # man - woman
show_similarity(4, 5)  # apple - mango
show_similarity(6, 7)  # football - cricket

print("\nCompare with unrelated words:")
show_similarity(0, 4)  # king - apple
show_similarity(1, 6)  # queen - football
show_similarity(3, 4)  # woman - apple
