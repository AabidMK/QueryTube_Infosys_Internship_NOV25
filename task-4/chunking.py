import pandas as pd
from transformers import AutoTokenizer

# ======================
# 1. Load data
# ======================
df = pd.read_csv("final_cleaned_merged.csv")

# ======================
# 2. Load tokenizer
# ======================
tokenizer = AutoTokenizer.from_pretrained("sentence-transformers/all-MiniLM-L6-v2")

# ======================
# 3. Chunking function
# ======================
def chunk_text(text, chunk_size=300, overlap=50):
    tokens = tokenizer.encode(text, add_special_tokens=False)

    chunks = []
    start = 0

    while start < len(tokens):
        end = start + chunk_size
        chunk_tokens = tokens[start:end]

        # convert tokens back to text
        chunk_text = tokenizer.decode(chunk_tokens)

        chunks.append(chunk_text)

        # move forward with overlap
        start = end - overlap

    return chunks

# ======================
# 4. Chunk each video
# ======================
chunk_rows = []

for idx, row in df.iterrows():
    video_id = row["id"]
    full_text = str(row["title"]) + " " + str(row["transcript"])

    chunks = chunk_text(full_text)

    for i, chunk in enumerate(chunks):
        chunk_rows.append({
            "video_id": video_id,
            "title": row["title"],
            "channel_title": row["channel_title"],
            "chunk_index": i,
            "chunk_text": chunk
        })

chunk_df = pd.DataFrame(chunk_rows)

# ======================
# 5. Save chunked data
# ======================
chunk_df.to_csv("chunked_data_with_titles.csv", index=False, encoding="utf-8")

print("🎉 Chunking complete!")
print("Total chunks created:", chunk_df.shape[0])
print("Saved as: chunked_data.csv")
