import chromadb
from google import genai
from google.genai import types

# 1. Configure Gemini 2.5
client = genai.Client(api_key="api_key")
MODEL_NAME = "gemini-2.5-flash" 

# 2. Load ChromaDB
client_chroma = chromadb.PersistentClient(path="./chroma_db")
collection = client_chroma.get_collection(name="youtube_videos")

# 3. Input Video ID
video_id = input("Enter video ID to summarize: ").strip()
result = collection.get(ids=[video_id], include=["documents", "metadatas"])

if not result["documents"]:
    print("Video ID not found")
    exit()

transcript = result["documents"][0]
title = result["metadatas"][0].get("title", "Unknown Title")

# 4. Prompt for a "Deep Narrative" Paragraph
# Adding "comprehensive" and "minimum 150 words" helps prevent small outputs.
prompt = f"""
Write one comprehensive, detailed narrative paragraph summarizing this video. 
Focus on the technical progression, specific tools mentioned (like HTML, CSS, JS), 
and the strategic advice given for long-term success. 

RULES:
- MUST be a single, long, meaningful paragraph.
- DO NOT use bullet points.
- Include specific details from the transcript; do not be generic.
- Aim for a depth that covers the 6-12 month roadmap described.

VIDEO TITLE: {title}
TRANSCRIPT: {transcript}
"""

# 5. Call Gemini with Thinking Enabled
try:
    response = client.models.generate_content(
        model=MODEL_NAME,
        contents=prompt,
        config=types.GenerateContentConfig(
            max_output_tokens=1000, # Higher limit to avoid truncation
            temperature=0.7,        # Higher temperature for better narrative flow
            # In 2025, use thinking_level to ensure the model processes deeply
            # thinking_level="high" 
        )
    )
    
    print("\n📄 DETAILED VIDEO SUMMARY\n")
    print(response.text.strip())

except Exception as e:
    print(f"Error: {e}")
