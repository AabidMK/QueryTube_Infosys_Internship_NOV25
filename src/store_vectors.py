import pandas as pd
import numpy as np
import faiss
import pickle
from sentence_transformers import SentenceTransformer

# Load data
df = pd.read_csv(
    r"E:\Internship\Infosys Springboard\Infosys Task1\INFOSYS TASK1\data\embedded_output1.csv"
)

# Use transcript (or combined_text if you prefer)
texts = df["transcript"].fillna("").tolist()

# Load embedding model
model = SentenceTransformer("all-MiniLM-L6-v2")

# Generate embeddings
embeddings = model.encode(texts, show_progress_bar=True)
embeddings = np.array(embeddings).astype("float32")

# Create FAISS index
dimension = embeddings.shape[1]
index = faiss.IndexFlatL2(dimension)
index.add(embeddings)

print(f"✅ Stored {index.ntotal} vectors of dimension {dimension}")

# Save index
faiss.write_index(index, "vector_index.faiss")

# Save metadata
metadata = df[[
    "id",
    "title",
    "channel_title",
    "viewCount",
    "duration"
]].rename(columns={
    "id": "video_id",
    "viewCount": "view_count"
}).to_dict(orient="records")

with open("metadata.pkl", "wb") as f:
    pickle.dump(metadata, f)

print("✅ Metadata saved")
