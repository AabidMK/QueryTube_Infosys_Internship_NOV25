import chromadb
import os
from groq import Groq
import textwrap

# ==============================
# 1. Configure Groq
# ==============================
groq_client = Groq(api_key=os.getenv("GROQ_API_KEY"))

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
    print("❌ No transcript found")
    exit()

full_transcript = " ".join(documents)

# ==============================
# 5. Split transcript into chunks
# ==============================
# ~2000 characters ≈ safe token size
chunks = textwrap.wrap(full_transcript, 2000)

print(f"\n🔹 Total chunks created: {len(chunks)}")

# ==============================
# 6. Summarize each chunk
# ==============================
chunk_summaries = []

for idx, chunk in enumerate(chunks, start=1):
    print(f"🔹 Summarizing chunk {idx}/{len(chunks)}")

    response = groq_client.chat.completions.create(
        model="llama-3.1-8b-instant",
        messages=[
            {"role": "system", "content": "Summarize the following text."},
            {"role": "user", "content": chunk}
        ],
        temperature=0.3
    )

    chunk_summaries.append(response.choices[0].message.content)

# ==============================
# 7. Final summary from chunk summaries
# ==============================
combined_summary_text = " ".join(chunk_summaries)

final_response = groq_client.chat.completions.create(
    model="llama-3.1-8b-instant",
    messages=[
        {"role": "system", "content": "Create a concise final summary from the following partial summaries."},
        {"role": "user", "content": combined_summary_text}
    ],
    temperature=0.3
)

# ==============================
# 8. Output
# ==============================
print("\n📝 FINAL VIDEO SUMMARY\n")
print(final_response.choices[0].message.content)
