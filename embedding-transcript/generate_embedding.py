import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer
import os

# ------------------------------
# CONFIG
# ------------------------------
INPUT_CSV = "../cleaned_metadata.csv"
OUTPUT_CSV = "transcript_embeddings.csv"
EMBEDDING_NPY = "embeddings.npy"

CHUNK_SIZE = 350   # ~350 words per chunk (safe for MiniLM)

# ------------------------------
# LOAD MODEL
# ------------------------------
print("Loading embedding model...")
model = SentenceTransformer("all-MiniLM-L6-v2")  
# Output dim = 384

# ------------------------------
# CHUNKING FUNCTION
# ------------------------------
def chunk_text(text, chunk_size=CHUNK_SIZE):
    """
    Splits text into chunks of 'chunk_size' words.
    Returns a list of text chunks.
    """
    words = text.split()
    return [" ".join(words[i:i+chunk_size]) for i in range(0, len(words), chunk_size)]


def embed_long_text(text):
    """
    Embeds long text by breaking it into chunks.
    Returns a single embedding (mean of chunk embeddings).
    """
    if not text.strip():
        return model.encode("")  # embed empty text safely
    
    chunks = chunk_text(text)

    # Embed each chunk
    chunk_embeddings = model.encode(chunks)

    # Take mean pooling over chunk embeddings
    return np.mean(chunk_embeddings, axis=0)


# ------------------------------
# READ DATA
# ------------------------------
df = pd.read_csv(INPUT_CSV)

if "transcript" not in df.columns:
    raise ValueError("❌ Transcript column not found in CSV!")

print(f"Loaded {len(df)} rows")

df["transcript"] = df["transcript"].fillna("")

# ------------------------------
# GENERATE EMBEDDINGS WITH CHUNKING
# ------------------------------
embeddings = []

print("Generating embeddings with chunking...")

for i, text in enumerate(df["transcript"]):
    if i % 10 == 0:
        print(f"Processing {i}/{len(df)}")

    emb = embed_long_text(text)
    embeddings.append(emb)

embeddings = np.array(embeddings)

# ------------------------------
# SAVE RESULTS
# ------------------------------
df["embedding"] = embeddings.tolist()

df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
np.save(EMBEDDING_NPY, embeddings)

print("\n✅ Embeddings generated successfully!")
print(f"📄 CSV saved as: {OUTPUT_CSV}")
print(f"💾 Numpy embeddings saved as: {EMBEDDING_NPY}")
print(f"🔢 Embedding shape: {embeddings.shape}")
