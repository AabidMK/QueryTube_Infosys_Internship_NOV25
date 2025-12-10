import pandas as pd
from sentence_transformers import SentenceTransformer
import ast

df = pd.read_csv("final_cleaned_merged.csv")

def combine_text(row):
    title = str(row["title"]) if pd.notna(row["title"]) else ""
    transcript = str(row["transcript"]) if pd.notna(row["transcript"]) else ""
    return title + " " + transcript

df["full_text"] = df.apply(combine_text, axis=1)

model = SentenceTransformer("all-MiniLM-L6-v2")

embeddings = model.encode(df["full_text"].tolist(), show_progress_bar=True)

df["embedding"] = embeddings.tolist()


df.to_csv("dataset_with_embeddings.csv", index=False)

print("Embeddings generated and saved successfully!")
