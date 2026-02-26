import pandas as pd
import ast
import numpy as np

main_path = '../Datasets/MainDataset.csv'
chunk_path = '../Datasets/trantitle.csv'
output_path = '../Datasets/Dataset_with_embeddings.csv'

main_df = pd.read_csv(main_path)
chunks_df = pd.read_csv(chunk_path)

chunks_df['embedding'] = chunks_df['embedding'].apply(lambda x: ast.literal_eval(x))

grouped_embeddings = (
    chunks_df.groupby('original_row')['embedding']
    .apply(lambda lists: np.mean(np.array(lists.tolist()), axis=0))
)

main_df['final_embedding'] = grouped_embeddings

# FIX: convert numpy array → python list so CSV won't truncate
main_df['final_embedding'] = main_df['final_embedding'].apply(lambda x: x.tolist())

main_df.to_csv(output_path, index=False)

print("Saved combined dataset →", output_path)
