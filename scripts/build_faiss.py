import pandas as pd, numpy as np, json, faiss

df = pd.read_csv("final_output_with_embeddings.csv")

vectors = np.vstack([
    np.array(json.loads(x), dtype='float32') 
    for x in df['embedding']
])

index = faiss.IndexFlatL2(vectors.shape[1])
index.add(vectors)

faiss.write_index(index, "videos.index")
df[['id', 'title']].to_csv("meta_for_index.csv", index=False)

print("Index saved!")

