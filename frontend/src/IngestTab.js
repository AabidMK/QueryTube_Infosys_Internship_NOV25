import React, { useState, useEffect } from "react";
import axios from "axios";

function IngestTab() {
  const [file, setFile] = useState(null);
  const [done, setDone] = useState(false);
  const [loading, setLoading] = useState(false);

  const handleFileChange = (e) => {
    setFile(e.target.files[0]);
    setDone(false);
  };

  const handleSubmit = async () => {
    if (!file) {
      alert("Please select a CSV file first.");
      return;
    }

    const formData = new FormData();
    formData.append("file", file);

    setLoading(true);
    setDone(false);

    try {
      await axios.post("http://127.0.0.1:8000/ingest", formData, {
        headers: { "Content-Type": "multipart/form-data" }
      });
      setDone(true);
    } catch (err) {
      alert("Upload failed: " + err.message);
    } finally {
      setLoading(false);
    }
  };

  // Auto-hide success message after 3 seconds
  useEffect(() => {
    if (done) {
      const timer = setTimeout(() => setDone(false), 3000);
      return () => clearTimeout(timer);
    }
  }, [done]);

  return (
    <div className="upload-section">
      <h2>Ingest CSV</h2>

      <label className="file-upload-label">
        <span className="file-upload-button">
          {file ? file.name : "Choose CSV File"}
        </span>
        <input
          type="file"
          accept=".csv"
          onChange={handleFileChange}
          style={{ display: "none" }}
        />
      </label>

      <button className="upload-btn" onClick={handleSubmit} disabled={loading}>
        {loading ? "Uploading..." : "Upload"}
      </button>

      {done && !loading && (
        <div className="upload-success">
          <span className="checkmark">✔</span>
          <span className="message">CSV successfully ingested</span>
        </div>
      )}
    </div>
  );
}

export default IngestTab;
