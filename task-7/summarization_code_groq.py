import chromadb
import os
from groq import Groq

# ==============================
# 1. Configure Groq Client
# ==============================
groq_client = Groq(
    api_key=os.getenv("GROQ_API_KEY")
)

# ==============================
# 2. Connect to ChromaDB
# ==============================
client = chromadb.PersistentClient(path="mydb")
collection = client.get_collection("semantic_search_tube")

# ==============================
# 3. Input video ID
# ==============================
video_id = input("Enter video ID: ")

# ==============================
# 4. Fetch transcript chunks
# ==============================
results = collection.get(
    where={"video_id": video_id},
    include=["documents"]
)

documents = results["documents"]

if not documents:
    print("❌ No transcript found for this video ID")
    exit()

# ==============================
# 5. Combine transcript
# ==============================
full_transcript = " ".join(documents)

print("\n📄 Transcript fetched successfully")
print(f"Transcript length: {len(full_transcript)} characters\n")

# ==============================
# 6. Summarization Prompt
# ==============================
prompt = f"""
Summarize the following YouTube video transcript clearly and concisely.
Focus on the main ideas and key takeaways.

Transcript:
{full_transcript}
"""

# ==============================
# 7. Generate summary using Groq (LLaMA-3)
# ==============================
response = groq_client.chat.completions.create(
    model="llama-3.1-8b-instant",
    messages=[
        {"role": "system", "content": "You are a helpful assistant that summarizes YouTube transcripts."},
        {"role": "user", "content": prompt}
    ],
    temperature=0.3
)

# ==============================
# 8. Output summary
# ==============================
print("📝 VIDEO SUMMARY\n")
print(response.choices[0].message.content)
