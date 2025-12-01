import pandas as pd

file1 = r"C:\Users\MADHAV\OneDrive\Desktop\ted-ed\output.csv"
file2 = r"C:\Users\MADHAV\OneDrive\Desktop\ted-ed\transcripts_output.csv"

df1 = pd.read_csv(file1)
df2 = pd.read_csv(file2)

merged_df = pd.merge(df1, df2, on="id", how="inner")

merged_df.to_csv(r"C:\Users\MADHAV\OneDrive\Desktop\ted-ed\final_merged.csv", index=False)

print("Merged successfully!")
