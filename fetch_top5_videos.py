import lancedb
from sentence_transformers import SentenceTransformer

# -------- CONFIG --------
DB_PATH = "lancedb_videos"
TABLE_NAME = "videos"
MODEL_NAME = "all-MiniLM-L6-v2"

def main():
    # 1️⃣ Accept user query
    query = input("Enter your search query: ").strip()

    # 2️⃣ Load embedding model
    print("Loading embedding model...")
    model = SentenceTransformer(MODEL_NAME)

    # 3️⃣ Convert query to embedding
    print("Generating query embedding...")
    query_embedding = model.encode(query).tolist()

    # 4️⃣ Connect to LanceDB
    print("Connecting to LanceDB...")
    db = lancedb.connect(DB_PATH)
    table = db.open_table(TABLE_NAME)

    # 5️⃣ Similarity search (get more rows for dedup)
    print("Searching similar videos...")
    results = (
        table.search(query_embedding)
        .limit(50)
        .to_pandas()
    )

    print("\nTop 5 Relevant Videos:\n")

    # 6️⃣ Deduplicate by video_id
    seen = set()
    rank = 1

    for _, row in results.iterrows():
        payload = row["payload"]
        video_id = payload.get("video_id")

        if video_id in seen:
            continue

        seen.add(video_id)

        print(f"Rank {rank}")
        print("Video ID       :", payload.get("video_id"))
        print("Title          :", payload.get("title"))
        print("Channel Name   :", payload.get("channel_title"))
        print("Similarity     :", round(row["_distance"], 4))
        print("-" * 40)

        rank += 1
        if rank > 5:
            break

if __name__ == "__main__":
    main()