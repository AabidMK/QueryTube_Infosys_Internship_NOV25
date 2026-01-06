# Query-Tube (AI Semantic Search Tube)

The goal of this project is to build a YouTube semantic search system by extracting and transforming video data via YouTube APIs.
The system allows users to input naltural language queries and receive the top-5 most semantically relavant video titles or IDs.

---

# Key features
1. Query Search
2. Semantic Matching
3. Similarity Score Display
4. Top Similar Videos Retrieval
5. Efficient Search Pipeline

---
# Steps
1. Data Extraction: 
- Fetch video metadata & channel information.
- Extract transcript/spoken content for smantic context. 
- Remove noisy duplicate and incomplete data.
- Final dataset with validated and structured text entries.
2. Embedding Generation:
- Input dta collected from YouTube videos(titles & transcripts).
- Remove noise, special characters, and irrelavant text.
- Generate dense vector representations capturing semantic context.
- Use FAISS/ChromaDB to store high-dimensional numerical embeddings.
3. Vector Storage & Indexing:
- Collection of numerical vectors derived from video titles and transcripts.
- FAISS, a central repository storing embeddings with metadata for retreival.
- Organise embeddings for fast similarity computations.
- Indexed data is prepared for real-time query-based retrieval. 
4. User Query & semantic Retrieval Flow:
- User enters a natural language search prompt.
- Converts the query into a numerical vector.
- Performs cosine similarity search on stored vectors.
- Ranks the most semantically relavant results.
 Displays titles, IDs, and previews to the user.

---
# Technologies Used
1. Frontend & Backend : HTML, CSS, React JS, Python
2. Tools & Environments : VS Code, Github
3. DataBase : FAISS (Facebook AI Similarity Search)
4. Model : Ollama
5. APIs : FastAPI, YouTube Data API v3, Flask, POSTMAN
