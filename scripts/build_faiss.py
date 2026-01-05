import pandas as pd
import numpy as np
import json
import faiss
import os

# ===============================
# PATHS
# ===============================
INPUT_CSV = "data/final_output_with_embeddings.csv"
VECTOR_DB_DIR = "vector_db"
INDEX_PATH = os.path.join(VECTOR_DB_DIR, "videos.index")
META_PATH = os.path.join(VECTOR_DB_DIR, "meta_for_index.csv")

os.makedirs(VECTOR_DB_DIR, exist_ok=True)

# ===============================
# LOAD CSV
# ===============================
df = pd.read_csv(INPUT_CSV)

# ===============================
# REQUIRED COLUMNS CHECK
# ===============================
required_cols = {"id", "title", "channel_title", "embedding", "combined_text"}
missing = required_cols - set(df.columns)

if missing:
    raise ValueError(f"❌ Missing columns in CSV: {missing}")

# ===============================
# BUILD FAISS VECTORS
# ===============================
vectors = np.vstack([
    np.array(json.loads(x), dtype="float32")
    for x in df["embedding"]
])

dimension = vectors.shape[1]
index = faiss.IndexFlatL2(dimension)
index.add(vectors)

faiss.write_index(index, INDEX_PATH)

# ===============================
# SAVE METADATA (FOR TASK 6 & API)
# ===============================
meta_df = df[
    ["id", "title", "channel_title", "combined_text"]
].copy()

meta_df.to_csv(META_PATH, index=False)

# ===============================
# DONE
# ===============================
print("✅ FAISS index built successfully")
print(f"🔢 Total vectors : {index.ntotal}")
print(f"📁 Index saved   : {INDEX_PATH}")
print(f"📄 Metadata cols: {meta_df.columns.tolist()}")
