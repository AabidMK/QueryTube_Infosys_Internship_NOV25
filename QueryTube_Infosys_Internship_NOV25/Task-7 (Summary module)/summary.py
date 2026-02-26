from google import genai
from dotenv import load_dotenv
import os
import textwrap
import chromadb
client = chromadb.PersistentClient(path='../Task6/chroma_db')
collection = client.get_collection(name='QueryTube')
def get_transcript_by_video_id(video_id):
    result = collection.get(
        where={"video_id": video_id}
    )
    
    if not result["documents"]:
        return None
    
    # Join in case transcript is split
    return " ".join(result["documents"])
# Load environment variables
load_dotenv('../Keys/.env')

# Get API key
Key = os.getenv('Gemini_Key')

# Create Gemini client
Client = genai.Client(api_key=Key)


#Chunking Function 
def chunk_text(text, chunk_size=500):
    """
    Splits large text into smaller chunks
    """
    return textwrap.wrap(text, chunk_size)

# Large Text Example 
text = get_transcript_by_video_id('r6jVezXnBMQ')
# Apply Chunking
chunks = chunk_text(text)

summaries = []

#  Summarize Each Chunk 
for chunk in chunks:
    response = Client.models.generate_content(
        model="gemini-2.5-flash",  # safest supported model
        contents=f"Summarize this text in detail:\n{chunk}"
    )
    summaries.append(response.text)

# Final Combined Summary 
final_summary = " ".join(summaries)

print("FINAL SUMMARY:\n")
print(final_summary)
