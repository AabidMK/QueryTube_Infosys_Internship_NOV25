# add_embeddings_local.py
import pandas as pd
from sentence_transformers import SentenceTransformer
from tqdm import tqdm
import json
import numpy as np

CSV_IN = "cleaned_youtube_data.csv"     # change if your file name differs
CSV_OUT = "kaggle_newembeddings1.csv"
TITLE_COL = "title"                 # change if your title column name is different
TRANSCRIPT_COL = "transcript"       # change if your transcript column name is different
EMBED_COL = "embedding"
MODEL_NAME = "all-MiniLM-L6-v2"
BATCH_SIZE = 32


def load_df(path):
    df = pd.read_csv(path, dtype=str)  # read as strings to avoid NaNs
    # ensure columns exist
    if TITLE_COL not in df.columns or TRANSCRIPT_COL not in df.columns:
        print("CSV columns:", df.columns.tolist())
        raise KeyError(f"Make sure columns '{TITLE_COL}' and '{TRANSCRIPT_COL}' exist.")
    df[[TITLE_COL, TRANSCRIPT_COL]] = df[[TITLE_COL, TRANSCRIPT_COL]].fillna("")
    return df

def combine_row(r):
    title = r[TITLE_COL].strip()
    transcript = r[TRANSCRIPT_COL].strip()
    if title and transcript:
        return title + " . " + transcript
    return title or transcript

def main():
    print("Loading CSV...")
    df = load_df(CSV_IN)
    print(f"Rows: {len(df)}")
    df["combined_text"] = df.apply(combine_row, axis=1)

    print("Loading sentence-transformers model:", MODEL_NAME)
    model = SentenceTransformer(MODEL_NAME)

    print("Computing embeddings in batches...")
    embeddings = []
    for i in tqdm(range(0, len(df), BATCH_SIZE), desc="Embedding"):
        texts = df["combined_text"].iloc[i:i+BATCH_SIZE].tolist()
        embs = model.encode(texts, show_progress_bar=False)
        embeddings.extend(embs)

    # save embedding as JSON string in new column
    df[EMBED_COL] = [json.dumps([float(x) for x in e]) for e in embeddings]

    # optional: save binary file for faster future loads
    np.save("embeddings.npy", np.array(embeddings))

    print("Writing CSV:", CSV_OUT)
    df.to_csv(CSV_OUT, index=False)
    print("Done. Saved CSV and embeddings.npy")

if __name__ == "__main__":
    main()