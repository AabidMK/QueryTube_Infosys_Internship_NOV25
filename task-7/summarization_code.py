import chromadb
import os
import google.generativeai as genai

# ==============================
# 1. Configure Gemini
# ==============================
genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))
model = genai.GenerativeModel("gemini-flash-latest")

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
    print("No transcript found")
    exit()

# ==============================
# 5. Combine transcript
# ==============================
full_transcript = " ".join(documents)

# ==============================
# 6. Prompt
# ==============================
prompt = f"""
Summarize the following YouTube video transcript.
Focus on key ideas and takeaways.

Transcript:
{full_transcript}
"""

# ==============================
# 7. Gemini summarization
# ==============================
response = model.generate_content(prompt)
print(response.text)
