import pandas as pd
import ast
import chromadb

# ============================
# ✅ Step 1 — Load dataset with embeddings
# ============================
INPUT_CSV = "dataset_with_embeddings.csv"
df = pd.read_csv(INPUT_CSV)

# Convert embedding string → list of floats
def parse_embedding(x):
    try:
        return ast.literal_eval(x)
    except:
        return None

df["embedding"] = df["embedding"].apply(parse_embedding)
df = df.dropna(subset=["embedding"])

# ✅ FIX: Replace NaN transcripts with empty string
df["transcript"] = df["transcript"].fillna("")

# ============================
# ✅ Step 2 — Initialize NEW ChromaDB client
# ============================
client = chromadb.PersistentClient(path="./chroma_db")

collection = client.get_or_create_collection(
    name="youtube_videos",
    metadata={"hnsw:space": "cosine"}
)

# ============================
# ✅ Step 3 — Insert rows into vector DB
# ============================
ids = df["id"].astype(str).tolist()
embeddings = df["embedding"].tolist()

metadatas = []
documents = []

for _, row in df.iterrows():
    metadatas.append({
        "title": row.get("title", ""),
        "channel_title": row.get("channel_title", ""),
        "view_count": int(row.get("viewcount", 0)),
        "duration": float(row.get("duration_seconds", 0)),
    })
    documents.append(str(row.get("transcript", "")))  # ✅ ensure string

collection.add(
    ids=ids,
    embeddings=embeddings,
    metadatas=metadatas,
    documents=documents
)

print("✅ All data inserted into ChromaDB successfully!")

# ============================
# ✅ Step 4 — Test similarity search
# ============================
query = "flying cars crash history"

from sentence_transformers import SentenceTransformer
model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

query_embedding = model.encode([query])[0]

results = collection.query(
    query_embeddings=[query_embedding],
    n_results=3
)

print("\n=== Top 3 Similar Videos ===")
for i in range(3):
    print(f"\nRank {i+1}:")
    print("Video ID:", results["ids"][0][i])
    print("Title:", results["metadatas"][0][i]["title"])
    print("Channel:", results["metadatas"][0][i]["channel_title"])
    print("View Count:", results["metadatas"][0][i]["view_count"])
