import { useState } from "react";
import API from "../api/api";

export default function IngestPage() {

  const [csvPath, setCsvPath] = useState("");
  const [status, setStatus] = useState("");

  const [file, setFile] = useState(null);

  const ingestPath = async () => {
    if (!csvPath.trim()) return;

    setStatus("Processing CSV... This can take several minutes...");

    try {
      await API.post("/ingest", { csv_path: csvPath });
      setStatus("✅ CSV ingestion successful!");
    } catch {
      setStatus("❌ Failed to ingest from path");
    }
  };

  const ingestFile = async () => {
    if (!file) return;

    const formData = new FormData();
    formData.append("file", file);

    setStatus("Uploading & processing... Please wait...");

    try {
      await API.post("/ingest-upload", formData, {
        headers: { "Content-Type": "multipart/form-data" }
      });

      setStatus("✅ File ingestion successful!");
    } catch {
      setStatus("❌ Upload failed");
    }
  };

  return (
    <div className="container" style={{ paddingTop: 70 }}>

      <h1 style={{ fontSize: 40, fontWeight: 800 }}>
        📦 Dataset Ingestion
      </h1>

      <p style={{ color: "#9ca3af", marginTop: 5 }}>
        Upload CSV or provide a file path
      </p>

      {/* File Upload */}
      <div style={{ marginTop: 40 }}>
        <h3>Upload CSV File</h3>

        <input
          type="file"
          accept=".csv"
          onChange={(e) => setFile(e.target.files[0])}
          style={{ marginTop: 10 }}
        />

        <br />

        <button
          onClick={ingestFile}
          className="button"
          style={{ background: "#22c55e", marginTop: 10 }}
        >
          Upload & Ingest
        </button>
      </div>

      <hr style={{ marginTop: 40, marginBottom: 40, borderColor: "#1f2937" }} />

      {/* CSV Path */}
      <div>
        <h3>Enter CSV Path</h3>

        <input
          className="input"
          value={csvPath}
          onChange={(e) => setCsvPath(e.target.value)}
          placeholder="C:/path/to/file.csv"
          style={{ marginTop: 10 }}
        />

        <br />

        <button
          onClick={ingestPath}
          className="button"
          style={{ background: "#38bdf8", marginTop: 10 }}
        >
          Ingest From Path
        </button>
      </div>

      <p style={{ marginTop: 30, fontSize: 18 }}>{status}</p>
    </div>
  );
}
