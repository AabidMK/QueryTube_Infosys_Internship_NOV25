import pandas as pd, numpy as np, json
from sentence_transformers import SentenceTransformer
from helpers import split_text_into_chunks

model = SentenceTransformer("all-MiniLM-L6-v2")   # 384-d
df = pd.read_csv("final_output.csv")             # input

embs = []
for _, r in df.iterrows():
    combined = (str(r.get("title","")) + " . " + str(r.get("transcript",""))).strip()
    chunks = split_text_into_chunks(combined, max_chars=1200, overlap_chars=300)
    if not chunks:
        vec = [0.0]*model.get_sentence_embedding_dimension()
    else:
        vecs = model.encode(chunks, show_progress_bar=False)
        vec = np.mean(vecs, axis=0).tolist()
    embs.append(json.dumps(vec))

df["embedding"] = embs
df.to_csv("final_output_with_embeddings.csv", index=False)
print("Saved final_output_with_embeddings.csv")
