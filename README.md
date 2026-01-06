# AI-QueryTube 🎥🤖

AI-QueryTube is a full-stack application that allows users to:
- Ingest YouTube data into a vector database
- Perform semantic search on videos
- Generate AI-based summaries of selected videos

---

## 🚀 Features

- CSV Ingestion into Vector DB
- Semantic Video Search
- AI-Generated Video Summaries
- Clean, modern React UI

---

## 🧰 Tech Stack

### Frontend
- React.js
- Axios
- CSS

### Backend
- FastAPI
- Python
- Qdrant (Vector Database)
- Sentence Transformers / Embeddings

---

## 📁 Project Structure

```text
AI-QueryTube/
│
├── backend/
│   ├── app/
│   │   ├── __init__.py
│   │   ├── main.py              # FastAPI entry point
│   │   ├── routes/
│   │   │   ├── ingest.py
│   │   │   ├── search.py
│   │   │   └── summary.py
│   │   ├── services/
│   │   │   ├── embeddings.py
│   │   │   ├── qdrant_client.py
│   │   │   └── youtube.py
│   │   └── utils/
│   │       └── helpers.py
│   │
│   ├── data/
│   │   ├── raw/
│   │   ├── processed/
│   │   └── samples/
│   │
│   ├── requirements.txt
│   └── README.md
│
├── frontend/
│   ├── public/
│   ├── src/
│   ├── package.json
│   ├── package-lock.json
│   └── README.md
│
├── docs/
│   ├── architecture.md
│   └── AI-QueryTube.pdf
│
├── scripts/
│   ├── fetch_metadata.py
│   ├── fetch_transcript.py
│   └── generate_embeddings.py
│
├── .gitignore
├── LICENSE
└── README.md

## ⚙️ Prerequisites

Ensure the following are installed on your system:

- **Node.js** (v16 or higher)
- **npm**
- **Python** (v3.9 or higher)
- **Git**
- **Qdrant** (running locally or via cloud)

---

## ▶️ Steps to Run the Project

### 1️⃣ Clone the Repository

```bash
git clone https://github.com/<your-username>/AI-QueryTube.git
cd AI-QueryTube

2️⃣ Run the Backend (FastAPI)

Make sure Qdrant is running before starting the backend.

cd backend
pip install -r requirements.txt
uvicorn app.main:app --reload

Backend will be availabe at :- http://127.0.0.1:8000

3️⃣ Run the Frontend (React)

Open a new terminal window and run:
cd frontend
npm install
npm start

Frontend will be available at:- http://localhost:3000

4️⃣ How to Use the Application

Open the frontend in your browser

Navigate to the Ingest tab and upload a CSV file

Use the Search tab to find semantically similar videos

Select a video and generate an AI-based Summary
