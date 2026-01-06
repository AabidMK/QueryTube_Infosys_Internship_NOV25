import { useState } from "react";
import "./App.css";

const BACKEND_URL = "http://127.0.0.1:8000";

function App() {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState([]);
  const [loading, setLoading] = useState(false);

  const [csvFile, setCsvFile] = useState(null);
  const [uploadStatus, setUploadStatus] = useState("");

  const [summary, setSummary] = useState("");
  const [summaryLoading, setSummaryLoading] = useState(false);
  const [summaryTitle, setSummaryTitle] = useState("");
  const [summaryLength, setSummaryLength] = useState("medium");

  // ---------------------------
  // INGEST
  // ---------------------------
  const uploadCSV = async () => {
    if (!csvFile) return;

    setUploadStatus("Uploading...");
    const formData = new FormData();
    formData.append("file", csvFile);

    const res = await fetch(`${BACKEND_URL}/ingest`, {
      method: "POST",
      body: formData,
    });

    const data = await res.json();
    setUploadStatus(`Ingested ${data.rows_ingested} rows`);
    setResults([]);
    setSummary("");
  };

  // ---------------------------
  // SEARCH
  // ---------------------------
  const searchVideos = async () => {
    setLoading(true);
    setSummary("");

    const res = await fetch(
      `${BACKEND_URL}/search?query=${encodeURIComponent(query)}`
    );
    const data = await res.json();

    setResults(data.results || []);
    setLoading(false);
  };

  // ---------------------------
  // SUMMARY BY VIDEO (WITH LENGTH)
  // ---------------------------
  const summarizeVideo = async (video) => {
    setSummaryLoading(true);
    setSummary("");
    setSummaryTitle(video.title);

    const res = await fetch(
      `${BACKEND_URL}/summarize/video/${video.video_id}?length=${summaryLength}`
    );
    const data = await res.json();

    setSummary(data.summary || "No summary available");
    setSummaryLoading(false);
  };

  return (
    <div className="app">
      <header className="header">
        <h1>🎥 QueryTube</h1>
        <p>Semantic Search & Video Summaries</p>
      </header>

      {/* CSV UPLOAD */}
      <section className="panel">
        <h3>📁 Upload CSV</h3>
        <input type="file" onChange={(e) => setCsvFile(e.target.files[0])} />
        <button onClick={uploadCSV}>Upload</button>
        {uploadStatus && <p className="status">{uploadStatus}</p>}
      </section>

      {/* SEARCH */}
      <section className="panel">
        <h3>🔍 Search</h3>
        <input
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="Search topic..."
        />
        <button onClick={searchVideos}>Search</button>
        {loading && <p>Searching...</p>}
      </section>

      {/* RESULTS */}
      <section className="results">
        {results.map((v) => (
          <div className="card" key={v.video_id}>
            <h4>{v.title}</h4>
            <p className="channel">{v.channel}</p>

            <div className="row">
              <select
                value={summaryLength}
                onChange={(e) => setSummaryLength(e.target.value)}
              >
                <option value="short">Short</option>
                <option value="medium">Medium</option>
                <option value="long">Long</option>
              </select>

              <button onClick={() => summarizeVideo(v)}>
                Summarize
              </button>
            </div>
          </div>
        ))}
      </section>

      {/* SUMMARY */}
      {(summaryLoading || summary) && (
        <section className="panel summary-panel">
          <h3>🧠 Summary</h3>
          <p className="muted">{summaryTitle}</p>
          {summaryLoading ? <p>Loading...</p> : <p>{summary}</p>}
        </section>
      )}
    </div>
  );
}

export default App;
