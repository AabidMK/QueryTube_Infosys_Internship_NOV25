import pandas as pd
from sentence_transformers import SentenceTransformer
import numpy as np
from transformers import AutoTokenizer


model_name = "sentence-transformers/all-MiniLM-L6-v2"
model = SentenceTransformer(model_name)
tokenizer = AutoTokenizer.from_pretrained(model_name)

MAX_TOKENS = 256  

def chunk_text(text, max_tokens=256):
    tokens = tokenizer.encode(text, add_special_tokens=False)
    
    chunks = []
    for i in range(0, len(tokens), max_tokens):
        chunk = tokens[i:i + max_tokens]
        chunk_text = tokenizer.decode(chunk)
        chunks.append(chunk_text)
    
    return chunks


def embed_long_text(text):
    chunks = chunk_text(text, MAX_TOKENS)

    chunk_embeddings = model.encode(chunks)
    final_embedding = np.mean(chunk_embeddings, axis=0)
    
    return final_embedding

df = pd.read_csv("final_merged_cleaned.csv")

df["combined"] = df["title"].fillna("") + " " + df["transcript"].fillna("")

df["embeddings"] = df["combined"].apply(lambda x: embed_long_text(x).tolist())

df.to_csv("final_with_embeddings.csv", index=False)

print("Saved as final_embeddings.csv")
