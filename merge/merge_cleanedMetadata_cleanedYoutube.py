import pandas as pd

csv1 = pd.read_csv("../cleaned_youtube_data.csv")
csv2 = pd.read_csv("../cleaned_metadata.csv")

merged = pd.concat([csv1, csv2], ignore_index=True)

merged.to_csv("merged_output.csv", index=False)
print("Merged successfully!")
