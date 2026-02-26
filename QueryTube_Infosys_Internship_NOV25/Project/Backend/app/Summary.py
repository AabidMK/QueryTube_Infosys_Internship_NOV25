from google import genai
from dotenv import load_dotenv
import os
import chromadb




def Summary(video_id):
 # Load environment variables
 load_dotenv('../Keys/.env')
 
 # ChromaDB client
 client = chromadb.PersistentClient(path="../Task6/chroma_db")
 collection = client.get_collection(name="QueryTube")

# Get API key
 Key = os.getenv("Gemini_Key")
 if not Key:
    raise ValueError("Gemini_Key not found in environment variables")

 #Create Gemini client
 Client = genai.Client(api_key=Key)
 result = collection.get(where={"video_id": video_id})

 if not result.get("documents"):
        return None

 text = " ".join(result["documents"])

 response = Client.models.generate_content(
        model="gemini-2.5-flash",
        contents=f"Summarize this transcript:\n{text}")

 return response.text
