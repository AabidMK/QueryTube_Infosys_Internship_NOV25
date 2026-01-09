# 🎥 QueryTube — Semantic Video Search & Summarization Platform

QueryTube is a **full‑stack semantic video search and summarization application**. It allows users to ingest video data, perform **meaning‑based search using vector embeddings**, and generate **LLM‑based summaries** for individual videos through an interactive UI.

This project demonstrates the integration of **FastAPI**, **React**, **vector databases**, **sentence transformers**, and **LLM APIs** in a real‑world system.

---

## 🚀 Features

- 📥 **CSV Ingestion**

  - Upload video metadata, transcripts, and embeddings
  - Persist data in a vector database (ChromaDB)

- 🔎 **Semantic Search**

  - Search videos by _meaning_, not keywords
  - Uses sentence‑transformer embeddings and cosine similarity

- 🧠 **LLM‑Based Summarization**

  - On‑demand video summaries
  - Powered by **Gemini 2.5 Flash lite (free‑tier)**

- 🖥️ **Interactive UI**

  - Custom video cards
  - Hover‑safe summary buttons
  - Draggable, resizable summary window
  - Minimize / Maximize / Close controls

---

## 🧱 Tech Stack

### Frontend

- React (Vite)
- JavaScript
- CSS (custom components)

### Backend

- FastAPI
- Python
- Pandas
- Sentence‑Transformers
- ChromaDB (persistent vector store)

### AI / ML

- `intfloat/multilingual-e5-large` (embeddings)
- **Gemini 2.5 Flash lite** (LLM summarization – free tier)

---

## 📂 Project Structure

```
QueryTube/
│
├── frontend/
│   ├── src/
│   │   ├── components/
│   │   │   ├── VideoCard.jsx
│   │   │   ├── SummaryWindow.jsx
│   │   │   └── NavBar.jsx
│   │   ├── api/api.js
│   │   └── css/
│   └── vite.config.js
│
├── backend/
│   ├── app.py
│   ├── Fetch.py
│   ├── Summary.py
│   ├── VectDb.py
│   └── chroma_db/
│
└── README.md
```

---

## ⚙️ How to Run the Project

### ✅ Prerequisites

- Python **3.9+**
- Node.js **18+**
- npm or yarn
- Google Gemini API key (free tier)

---

## 🐍 Backend Setup (FastAPI)

### 1️⃣ Create virtual environment

```bash
python -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate
```

### 2️⃣ Install dependencies

```bash
pip install fastapi uvicorn pandas chromadb sentence-transformers python-dotenv google-generativeai
```

### 3️⃣ Set environment variables

Create a `.env` file in the backend directory:

```
GOOGLE_API_KEY=your_gemini_api_key_here
```

### 4️⃣ Run the backend

```bash
uvicorn app:app --reload
```

Backend will run at:

```
http://localhost:8000
```

---

## ⚛️ Frontend Setup (React + Vite)

### 1️⃣ Install dependencies

```bash
cd frontend
npm install
```

### 2️⃣ Run the frontend

```bash
npm run dev
```

Frontend will run at:

```
http://localhost:5173
```

---

## 📥 CSV Ingestion Format

Your CSV file should contain (at minimum):

- `id` – video ID
- `title`
- `channel_title`
- `transcript`
- `final_embedding` (list of floats as string)

Example:

```csv
id,title,channel_title,transcript,final_embedding
abc123,Intro to ML,ML Channel,"This video explains...","[0.12, 0.98, ...]"
```

---

## 🔎 How Search Works

1. User query → embedding via sentence‑transformer
2. Embedding compared against stored vectors
3. Top‑K most similar videos returned
4. Results ranked by similarity score

---

## 🧠 How Summarization Works

1. User clicks **Summarize** on a video
2. Video ID sent to backend
3. Transcript retrieved
4. Prompt sent to **Gemini 1.5 Flash**
5. Summary returned and displayed in draggable window

---

## 🧪 Known Limitations

- Free‑tier LLMs may have rate limits
- Large transcripts may require chunking
- No authentication (by design for demo)

---

## 📌 Future Improvements

- Authentication & user sessions
- Background ingestion jobs
- Caching summaries
- Pagination & filters
- Deployment (Docker + Cloud)

---

## 👤 Author

**Karan**
Semantic Search & AI Systems Project

---

## 📜 License

This project is for educational and demonstration purposes.
