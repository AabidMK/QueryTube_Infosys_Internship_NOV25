# 🎥 QueryTube – AI-Powered YouTube Semantic Search & Summarization

QueryTube is an AI-driven application that enables **semantic search** and **intelligent summarization** of YouTube videos using **vector embeddings**, **ChromaDB**, and **Generative AI**.

Instead of traditional keyword-based search, QueryTube understands the **meaning** of queries and retrieves the most relevant videos.

---

## 🧠 How Semantic Search Works

1. User enters a natural language query  
2. Query is converted into a vector embedding  
3. ChromaDB compares it with stored embeddings  
4. Cosine similarity is used for ranking  
5. Top results are returned with similarity scores  

---

## 🚀 Key Features

- 📂 CSV-based dataset ingestion  
- 🔍 Meaning-based semantic search  
- 📊 Similarity score for each result  
- 🧠 Vector storage using ChromaDB  
- 📝 AI-generated video summaries using Gemini  
- 🖥️ Interactive React frontend  

---

## 🏗️ System Architecture

```text
QueryTube/
│
├── fastapi/
│   ├── main.py            # FastAPI entry point
│   ├── ingest.py          # CSV ingestion API
│   ├── search.py          # Semantic search logic
│   ├── summarize.py       # Video summarization
│   └── QueryTube_db/      # ChromaDB persistent storage
│
├── embedding-transcript/
│   ├── generate_embedding.py
│   ├── title_transcript_embeddings.npy
│
├── merge/
│   ├── final_clean_dataset.csv
│   └── merge scripts
│
├── querytube-ai/          # React frontend
│   ├── src/
│   ├── public/
│   └── package.json
│
├── youtube_scrapper.py    #fetch youtube videos data
├── fetch_serpapi_transcripts.py    #fetch transcripts form video id
├── cleaned_metadata.csv
├── videos_metadata.csv
├── README.md
└── .gitignore


---

## ⚙️ Setup Instructions

### Backend Setup
cd fastapi
python -m venv .venv
.venv\Scripts\activate   # Windows
pip install -r requirements.txt
uvicorn main:app --reload

### Frontend Setup
cd querytube-ai
npm install
npm start
