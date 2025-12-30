import pandas as pd 
from sentence_transformers import SentenceTransformer 
import ast
 # Load CSV
df = pd.read_csv("final_output.csv")
 # Load embedding model
model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2") 
# Function to combine text 
def combine_text(row): 
    title = str(row["title"]) 
    transcript = str(row["transcript"]) 
    return title + " " + transcript 
# simple concatenation 
# Create a new column "combined_text"
df["combined_text"] = df.apply(combine_text, axis=1) 

# Generate embeddings
embeddings = model.encode(df["combined_text"].tolist(), show_progress_bar=True) 

# Add embeddings as a new column 
df["embeddings"] = embeddings.tolist() 
# Save back to CSV 

df.to_csv("youtube_with_embeddings.csv", index=False)
print("Embeddings saved successfully!")