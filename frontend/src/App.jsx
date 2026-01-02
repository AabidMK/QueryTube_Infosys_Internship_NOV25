import { useState } from "react";
import "./App.css";

const API_BASE = "http://127.0.0.1:8000";

export default function App() {
  const [activeTab, setActiveTab] = useState("ingest");

  return (
    <div className="container">
      <h1 className="title">Querytube Dashboard</h1>

      <div className="tabs">
        <button className={activeTab === "ingest" ? "active" : ""} onClick={() => setActiveTab("ingest")}>Ingest</button>
        <button className={activeTab === "search" ? "active" : ""} onClick={() => setActiveTab("search")}>Search</button>
        <button className={activeTab === "summarize" ? "active" : ""} onClick={() => setActiveTab("summarize")}>Summarize</button>
      </div>

      {activeTab === "ingest" && <Ingest />}
      {activeTab === "search" && <Search />}
      {activeTab === "summarize" && <Summarize />}
    </div>
  );
}

/* ---------------- INGEST ---------------- */

function Ingest() {
  const [file, setFile] = useState(null);
  const [status, setStatus] = useState("");

  const uploadCSV = async () => {
    if (!file) {
      setStatus("Please select a CSV file");
      return;
    }

    setStatus("Uploading...");

    const formData = new FormData();
    formData.append("file", file);

    const res = await fetch(`${API_BASE}/ingest-csv`, {
      method: "POST",
      body: formData,
    });

    const data = await res.json();
    setStatus(data.message);
  };

  return (
    <div className="card">
      <h2>CSV Ingestion</h2>
      <input type="file" accept=".csv" onChange={e => setFile(e.target.files[0])} />
      <button onClick={uploadCSV}>Upload</button>
      <p className="status">{status}</p>
    </div>
  );
}

/* ---------------- SEARCH ---------------- */

function Search() {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState([]);
  const [status, setStatus] = useState("");

  const search = async () => {
    if (!query.trim()) {
      setStatus("Please enter a search query");
      setResults([]);
      return;
    }

    setStatus("Searching...");
    setResults([]);

    const res = await fetch(`${API_BASE}/search`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query, top_k: 5 }),
    });

    const data = await res.json();

    if (data.length === 0) {
      setStatus("No results found");
    } else {
      setStatus("");
      setResults(data);
    }
  };

  return (
    <div className="card full">
      <h2>Search Videos</h2>

      <input
        type="text"
        placeholder="Enter search text"
        value={query}
        onChange={e => setQuery(e.target.value)}
      />

      <button onClick={search}>Search</button>
      <p className="status">{status}</p>

      <div className="results">
        {results.map(video => (
          <div key={video.video_id} className="result-row">

            <a
              href={`https://www.youtube.com/watch?v=${video.video_id}`}
              target="_blank"
              rel="noreferrer"
            >
              <img
                src={`https://img.youtube.com/vi/${video.video_id}/hqdefault.jpg`}
                alt="thumbnail"
              />
            </a>

            <div className="info">
              <h3>{video.title}</h3>
              <p><b>Video ID:</b> {video.video_id}</p>
              <p><b>Channel:</b> {video.channel}</p>
              <p><b>Views:</b> {video.view_count}</p>
              <p><b>Duration:</b> {video.duration_seconds}s</p>
              <p><b>Similarity:</b> {video.similarity_score?.toFixed(4)}
</p>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

/* ---------------- SUMMARIZE ---------------- */

function Summarize() {
  const [videoId, setVideoId] = useState("");
  const [summary, setSummary] = useState("");
  const [status, setStatus] = useState("");

  const summarize = async () => {
    if (!videoId.trim()) {
      setStatus("Please enter a video ID");
      return;
    }

    setStatus("Generating summary...");
    setSummary("");

    const res = await fetch(`${API_BASE}/summarize`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ video_id: videoId }),
    });

    if (!res.ok) {
      const err = await res.json();
      setStatus(err.detail);
      return;
    }

    const data = await res.json();
    setStatus("");
    setSummary(data.summary);
  };

  return (
    <div className="card">
      <h2>Summarize Video</h2>

      <input
        type="text"
        placeholder="Enter Video ID"
        value={videoId}
        onChange={e => setVideoId(e.target.value)}
      />

      <button onClick={summarize}>Summarize</button>
      <p className="status">{status}</p>

      {summary && <div className="summary-box">{summary}</div>}
    </div>
  );
}
