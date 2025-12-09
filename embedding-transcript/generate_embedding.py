import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer
import os
import re

# ---------------------------------------
# CONFIG
# ---------------------------------------
INPUT_CSV = "../cleaned_metadata.csv"
OUTPUT_CSV = "title_transcript_embeddings.csv"
EMBEDDING_NPY = "title_transcript_embeddings.npy"

CHUNK_SIZE = 350  # words per chunk

# ---------------------------------------
# LOAD MODEL
# ---------------------------------------
print("Loading MiniLM model...")
model = SentenceTransformer("all-MiniLM-L6-v2")  # 384-dim embeddings

# ---------------------------------------
# CHUNKING FUNCTION
# ---------------------------------------
def chunk_text(text, chunk_size=350):
    text = re.sub(r'\s+', ' ', text).strip()
    words = text.split()

    if len(words) == 0:
        return [""]

    chunks = []
    for i in range(0, len(words), chunk_size):
        chunk = " ".join(words[i : i + chunk_size])
        chunks.append(chunk)
    return chunks

# ---------------------------------------
# LOAD DATA
# ---------------------------------------
df = pd.read_csv(INPUT_CSV)
df["transcript"] = df["transcript"].fillna("").astype(str)
df["title"] = df["title"].fillna("").astype(str)

print(f"Loaded {len(df)} rows")

# ---------------------------------------
# EMBEDDING GENERATION
# ---------------------------------------
final_embeddings = []

print("\nGenerating embeddings...")

for idx, row in df.iterrows():
    title = row["title"]
    transcript = row["transcript"]

    if idx % 10 == 0:
        print(f"Processing {idx}/{len(df)}")

    # ---- Title Embedding ----
    title_embedding = model.encode(title)

    # ---- Transcript Chunk Embeddings ----
    chunks = chunk_text(transcript, CHUNK_SIZE)
    chunk_embeddings = model.encode(chunks)

    # Mean pooling for transcript
    if isinstance(chunk_embeddings, list):
        transcript_embedding = np.mean(chunk_embeddings, axis=0)
    else:
        transcript_embedding = chunk_embeddings.mean(axis=0)

    # ---- Combine Title + Transcript ----
    final_vector = np.concatenate([title_embedding, transcript_embedding])  # 768-dim

    final_embeddings.append(final_vector)


final_embeddings = np.array(final_embeddings)

# ---------------------------------------
# SAVE RESULTS
# ---------------------------------------
df["title_transcript_embedding"] = final_embeddings.tolist()
df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")

np.save(EMBEDDING_NPY, final_embeddings)

print("\n✅ Embeddings generated successfully!")
print(f"CSV saved as: {OUTPUT_CSV}")
print(f"NPY vector file saved as: {EMBEDDING_NPY}")
print("Embedding shape:", final_embeddings.shape)
