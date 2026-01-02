import { useState } from "react";
import axios from "axios";

export default function Ingestion() {
  const [file, setFile] = useState(null);
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState("");

  const uploadCSV = async () => {
    if (!file) {
      setMessage("❌ Please select a CSV file first.");
      return;
    }

    const formData = new FormData();
    formData.append("file", file);

    try {
      setLoading(true);
      setMessage("");

      const res = await axios.post(
        "http://127.0.0.1:8000/ingest/csv",
        formData,
        { headers: { "Content-Type": "multipart/form-data" } }
      );

      setMessage(
        `✅ Upload successful! Vectors inserted: ${res.data.vector_inserted}`
      );
    } catch (err) {
      console.error(err);
      setMessage("❌ CSV ingestion failed. Check backend.");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="section-card">
      <h2>CSV Ingestion</h2>
      <p>Select a CSV file to ingest data into Qdrant.</p>

      <input
        type="file"
        accept=".csv"
        onChange={(e) => setFile(e.target.files[0])}
      />

      <div style={{ marginTop: "20px" }}>
        <button onClick={uploadCSV} disabled={loading}>
          {loading ? "Uploading..." : "Upload CSV"}
        </button>
      </div>

      {message && <p className="message">{message}</p>}
    </div>
  );
}
