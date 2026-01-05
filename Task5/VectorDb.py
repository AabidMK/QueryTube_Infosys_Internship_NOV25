import chromadb
from chromadb import PersistentClient
# for loading the csv file
import pandas as pd
#for converting python object looking string to python
import ast

# read the csv as db
df= pd.read_csv('../Datasets/Dataset_with_embeddings.csv')
client=chromadb.PersistentClient(path='chroma_db')
collec = client.create_collection(name="QueryTube")
#ok so maybe in vector db list remains separate as list so we can work later 
embeds = df['final_embedding'].apply(lambda x:ast.literal_eval(x)).tolist()

#ok so chroma vector db can only store 3 things embeddings document and metadata
ids = df['id'].astype(str).tolist()

metadata=[]

for _,row in df.iterrows():
    metadata.append({
        "video_id":row['id'],
        "title":row['title'],
        "transcript": row["transcript"],
        "channel_title": row["channel_title"],
        "view_count": row["viewCount"],
        "duration": row["duration_seconds"],
        "Upload_date":row["publishedAt"],
        "thumbnail":row['thumbnail_high'],
        "likes":row['likeCount']
    })
document=df['transcript'].tolist()
collec.add(ids=ids,embeddings=embeds,metadatas=metadata,documents=document)
print("Successfull")