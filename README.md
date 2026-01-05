# QueryTube: AI-Powered YouTube Video Search

A full-stack application that allows you to perform semantic search and get AI-powered summaries for a collection of YouTube videos.

## Folder Structure

```
.
├── backend/
│   ├── api/
│   ├── embeddings/
│   ├── preprocessing/
│   ├── search/
│   ├── summarizer/
│   ├── vectordb/
│   └── app.py
├── frontend/
│   ├── public/
│   └── src/
│       ├── api/
│       ├── components/
│       └── pages/
├── data/
│   ├── processed/
│   └── raw/
├── .gitignore
├── package-lock.json
└── requirements.txt
```

## Features

- **Data Ingestion**: Ingest YouTube video data from a CSV file. The backend processes the data, fetches transcripts, generates embeddings, and stores them in a vector database.
- **Semantic Search**: Search for videos based on semantic meaning rather than just keywords.
- **AI-Powered Summarization**: Generate a concise, bullet-point summary for any video in the database.
- **Web Interface**: A clean and simple web interface to interact with the backend services.

## Tech Stack

- **Backend**: Python, FastAPI, ChromaDB, Sentence-Transformers, Google Generative AI
- **Frontend**: React.js, Vite, Axios
- **Database**: ChromaDB (Vector Database)

## How to Run

### Backend

1.  **Install dependencies**:
    ```bash
    pip install -r requirements.txt
    ```
2.  **Run the server**:
    ```bash
    uvicorn backend.app:app --reload
    ```
    The backend will be running at `http://127.0.0.1:8000`.

### Frontend

1.  **Navigate to the frontend directory**:
    ```bash
    cd frontend
    ```
2.  **Install dependencies**:
    ```bash
    npm install
    ```
3.  **Run the development server**:
    ```bash
    npm run dev
    ```
    The frontend will be running at `http://localhost:5173`.

### Usage

1.  Use the "Ingest" page to load a CSV file with YouTube video data.
2.  Use the "Search" page to perform a semantic search on the ingested videos.
3.  From the search results, you can click "Summary" to get an AI-generated summary of the video.

## Notes

- The project requires a `cleaned_youtube_data.csv` file in the `data/raw` directory for ingestion, or you can provide a path to a similar CSV file.
- The backend uses Google's Generative AI, which requires an API key and proper configuration.
- The system is designed to work with YouTube videos that have available transcripts.
- The initial data ingestion and embedding generation can be time-consuming depending on the size of the dataset.

