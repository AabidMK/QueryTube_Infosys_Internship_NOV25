import pandas as pd, json
df = pd.read_csv("final_output_with_embeddings.csv")
print("Rows:", len(df))
print("Embedding dimension:", len(json.loads(df['embedding'].iloc[0])))

