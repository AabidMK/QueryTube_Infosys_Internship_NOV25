from sentence_transformers import SentenceTransformer
import pandas as pd
import numpy as np
import ast
import nltk

# Download tokenizer once
nltk.download('punkt')

# Load the embedding model
model = SentenceTransformer('intfloat/multilingual-e5-large')

# ---- Chunking Function ----
def chunk_text(text, chunk_size=300, overlap=70):
    """
    Simple character-based chunker with overlap.
    Breaks text into overlapping chunks for better embedding.
    """
    chunks = []
    start = 0
    text_len = len(text)

    while start < text_len:
        end = start + chunk_size
        chunk = text[start:end]
        chunks.append(chunk)

        # Move forward but overlap a little to preserve meaning
        start = end - overlap
        if start < 0:
            start = 0

    return chunks


# ---- Paths ----
main_path = '../Datasets/MainDataset.csv'
chunks_output_path = '../Datasets/trantitle.csv'
final_output_path = '../Datasets/MainDataset_with_final_embeddings.csv'


# ---- Load Main Dataset ----
df = pd.read_csv(main_path)
df['title_transcript'] = df['title'].fillna('') + ' ' + df['transcript'].fillna('')


# ---- Create Chunks ----
all_chunks = []
orig_row_index = []

for idx, text in enumerate(df['title_transcript']):
    chunks = chunk_text(text, chunk_size=300, overlap=70)

    for c in chunks:
        all_chunks.append(c)
        orig_row_index.append(idx)


# ---- Embed All Chunks ----
embeddings = model.encode(
    all_chunks,
    batch_size=32,
    show_progress_bar=True,
    normalize_embeddings=False
)

# ---- Save Chunk-Level Output ----
chunk_df = pd.DataFrame({
    "original_row": orig_row_index,
    "chunk_text": all_chunks,
    "embedding": [emb.tolist() for emb in embeddings]
})

chunk_df.to_csv(chunks_output_path, index=False)
print("Chunk-level dataset saved at:", chunks_output_path)


# ---- Combine Chunk Embeddings Per Document ----
# Convert embeddings from strings (list text) → actual list of floats
chunk_df['embedding'] = chunk_df['embedding'].apply(lambda x: np.array(ast.literal_eval(x)))

# Group by original row and average embeddings
grouped_embeddings = (
    chunk_df.groupby('original_row')['embedding']
    .apply(lambda vectors: np.mean(np.vstack(vectors), axis=0))
)

# Add to main dataset
df['final_embedding'] = grouped_embeddings

# ---- Save Final Output ----
df.to_csv(final_output_path, index=False)
print("Final dataset with embeddings saved at:", final_output_path)
