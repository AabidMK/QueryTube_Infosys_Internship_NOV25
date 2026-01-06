# 🎯 Semantic AI Search Tube
*(Infosys Springboard Internship Project)*

## 📌 Project Overview
Semantic AI Search Tube is an AI-powered system that enables users to search
YouTube video content based on **meaning and intent**, rather than exact keywords.

The project uses **sentence embeddings** and a **vector database** to retrieve
the most relevant video segments along with timestamps.

---

## 🚀 Motivation
Traditional keyword-based search often fails to capture user intent,
especially when content is buried deep inside long videos.
This project addresses that limitation using semantic search techniques.

---

## 🧠 Technologies Used

### Backend
- Python
- FastAPI
- SentenceTransformers
- ChromaDB (Vector Database)

### Frontend
- React.js
- Tailwind CSS
- Axios
- NPM

---

## 🗂️ Project Structure
```
semantic-ai-search-tube/
├── task-1/
├── task-2/
├── task-3/
├── task-4/
├── task-5/
├── task-6/
├── task-7/
├── task-8/
├── requirements.txt
├── README.md
└── .gitignore
```

---

## 📊 Dataset Information
- YouTube video transcripts were collected and processed
- Text was split into meaningful chunks
- Embeddings were generated using SentenceTransformer models
- Embeddings are stored in **ChromaDB**

⚠️ **Note:**  
Raw CSV files used during intermediate processing are not included
in this repository to avoid large file uploads.
They are required only during the data ingestion phase.

---

## ⚙️ System Workflow
1. Collect YouTube video transcripts
2. Preprocess and clean text data
3. Split transcripts into chunks
4. Generate embeddings for each chunk
5. Store embeddings in ChromaDB
6. Convert user query into an embedding
7. Perform semantic similarity search
8. Display relevant video segments with timestamps

---

## 🎯 Key Features
- Semantic (intent-based) search
- Accurate video segment retrieval
- Timestamp-based navigation
- Scalable vector search architecture

---

## ▶️ How to Run the Project

### Backend
```bash
pip install -r requirements.txt
uvicorn api:app --reload
```

### Frontend
```bash
npm install
npm start
```

---

## 📝 Conclusion
This project demonstrates the practical use of **AI and NLP techniques**
to enhance content discovery in large video platforms using semantic search.

---

## 🙌 Acknowledgement
Developed as part of the **Infosys Springboard Internship Program**.
