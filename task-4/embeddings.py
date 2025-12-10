import pandas as pd
from sentence_transformers import SentenceTransformer

# =========================
# 1. Load chunked data
# =========================
df = pd.read_csv("chunked_data.csv")

print("Loaded:", df.shape)

# =========================
# 2. Load embedding model
# =========================
model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

# =========================
# 3. Generate embeddings
# =========================
embeddings = model.encode(
    df["chunk_text"].tolist(),
    show_progress_bar=True
)

# Save as list in new column
df["chunk_embedding"] = embeddings.tolist()

# =========================
# 4. Save final result
# =========================
df.to_csv("chunked_embeddings_new.csv", index=False, encoding="utf-8")

print("\n🎉 Embeddings done!")
print("Saved as: chunked_embeddings.csv")
