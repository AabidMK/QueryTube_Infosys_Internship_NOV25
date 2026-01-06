import pandas as pd

df = pd.read_csv("final_cleaned_merged_chunks.csv")

# keep only first 2 rows
df = df.head(3)

df.to_csv("small_chunks.csv", index=False)
print("Done! Saved as small_chunks.csv")
