import pandas as pd
import chromadb
from sentence_transformers import SentenceTransformer
model = SentenceTransformer('intfloat/multilingual-e5-large')

#i think this persistantclient creates or load the dataset without creating it itself
client = chromadb.PersistentClient(path='../Task6/chroma_db')
#z = input("enter the query:")

#collection name is like table name so should be equal to already 
#existed collection
#print(client.list_collections())
collection = client.get_collection(name='QueryTube')



i = int(input("enter the number of runs: "))
for i in range(i):
        
    query = input("Enter your query: ")

    query_embedding = model.encode(
        f"query: {query}",
        normalize_embeddings=True
    ).tolist()

    results = collection.query(
        query_embeddings=[query_embedding],
        n_results=5,
        include=["metadatas", "distances"]
    )

    print("\nTop Results:")
    for i, meta in enumerate(results["metadatas"][0], start=1):
        similarity = round(1 - results["distances"][0][i-1], 4)
        #yeah meta['channel_title'] not working but get is working
        print(f"{i}. {meta['title']}  Channel = ({meta.get('channel_title')})")
        print(f"   Video ID: {meta['video_id']}")
        print(f"   Similarity: {similarity}\n")

    print("_" * 60)
