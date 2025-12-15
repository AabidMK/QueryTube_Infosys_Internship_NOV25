import pandas as pd
from sentence_transformers import SentenceTransformer

# ============================
# ✅ Step 1 — Load cleaned dataset
# ============================
INPUT_CSV = "final_clean_dataset.csv"
OUTPUT_CSV = "dataset_with_embeddings.csv"

df = pd.read_csv(INPUT_CSV)

# ============================
# ✅ Step 2 — Load embedding model
# ============================
print("Loading embedding model...")
model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

# ============================
# ✅ Step 3 — Choose text to embed
# We will embed: transcript + title (combined)
# ============================
def build_text(row):
    title = str(row.get("title", ""))
    transcript = str(row.get("transcript", ""))
    return title + " " + transcript

df["text_for_embedding"] = df.apply(build_text, axis=1)

# ============================
# ✅ Step 4 — Generate embeddings
# ============================
print("Generating embeddings...")

embeddings = model.encode(
    df["text_for_embedding"].tolist(),
    show_progress_bar=True
)

# Convert embeddings (numpy arrays) → Python lists for saving
df["embedding"] = embeddings.tolist()

# ============================
# ✅ Step 5 — Save updated dataset
# ============================
df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")

print(f"✅ Embeddings generated and saved to {OUTPUT_CSV}")
