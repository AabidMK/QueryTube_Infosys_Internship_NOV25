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

QueryTube/
│
├── fastapi/
│ ├── main.py
│ ├── ingest.py
│ ├── search.py
│ ├── summarize.py
│ └── QueryTube_db/
│
├── embedding-transcript/
│ ├── generate_embedding.py
│ └── title_transcript_embeddings.npy
│
├── merge/
│ └── final_clean_dataset.csv
│
├── querytube-ai/
│ ├── src/
│ ├── public/
│ └── package.json
│
├── README.md
└── .gitignore
