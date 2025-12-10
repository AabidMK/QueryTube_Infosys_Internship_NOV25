import pandas as pd
from transformers import AutoTokenizer

# Load your dataset
df = pd.read_csv("final_cleaned_merged.csv")

# Load tokenizer (choose model you used for embeddings)
tokenizer = AutoTokenizer.from_pretrained("sentence-transformers/all-MiniLM-L6-v2")

# Function to count tokens
def count_tokens(text):
    if pd.isna(text):
        return 0
    tokens = tokenizer.encode(text, add_special_tokens=False)
    return len(tokens)

# Combine title + transcript
df["full_text"] = df["title"].fillna("") + " " + df["transcript"].fillna("")

# Count tokens for each video
df["token_count"] = df["full_text"].apply(count_tokens)

# Print top 10
print(df[["id", "token_count"]].head())

# Save results if needed
df.to_csv("token_counts.csv", index=False)

print("\n🎉 Token count done! Saved as token_counts.csv")
