import { useState } from "react";
import axios from "axios";

export default function IngestPage() {
  const [csvPath, setCsvPath] = useState("");
  const [file, setFile] = useState(null);
  const [message, setMessage] = useState("");

  const API_BASE = "http://127.0.0.1:8000";

  /* --------------------------------------------------
      1️⃣ INGEST VIA PATH
  -------------------------------------------------- */
  const handlePathIngest = async () => {
    if (!csvPath.trim()) {
      setMessage("⚠️ Please enter a CSV path!");
      return;
    }

    try {
      const res = await axios.post(`${API_BASE}/ingest-path?csv_path=${csvPath}`);
      alert("RESPONSE:\n" + JSON.stringify(res.data, null, 2));
      setMessage(`🎉 Path ingestion successful! Rows ingested: ${res.data.rows}`);
    } catch (err) {
      console.error(err);
      setMessage("❌ Ingestion failed (Path). Check console.");
    }
  };

  /* --------------------------------------------------
      2️⃣ INGEST VIA FILE UPLOAD
  -------------------------------------------------- */
  const handleFileIngest = async () => {
    if (!file) {
      setMessage("⚠️ Please select a CSV file first!");
      return;
    }

    const formData = new FormData();
    formData.append("file", file);

    try {
      const res = await axios.post(`${API_BASE}/ingest-upload`, formData, {
        headers: { "Content-Type": "multipart/form-data" }
      });

      alert("UPLOAD RESULT:\n" + JSON.stringify(res.data, null, 2));
      setMessage(`📁🎉 File upload successful! Rows ingested: ${res.data.rows}`);
    } catch (err) {
      console.error(err);
      setMessage("❌ Ingestion failed (File Upload). Check console.");
    }
  };


  return (
    <div className="p-6 max-w-lg mx-auto">

      <h1 className="text-3xl font-bold text-center mb-6 text-blue-600">
        Ingest Data📥
      </h1>

      {/* 🟢 INGEST BY PATH */}
      <div className="mb-10 border p-4 rounded-lg shadow">
        <h2 className="text-xl font-semibold mb-2">📌 Ingest via CSV Path</h2>

        <input
          className="border w-full p-2 mb-3 rounded"
          type="text"
          placeholder="Enter path... e.g. C:/Users/MADHAV/Desktop/file.csv"
          value={csvPath}
          onChange={(e) => setCsvPath(e.target.value)}
        />

        <button
          onClick={handlePathIngest}
          className="bg-blue-600 text-white px-4 py-2 rounded hover:bg-blue-700"
        >
          Ingest from Path
        </button>
      </div>

      {/* 🟣 INGEST BY FILE UPLOAD */}
      <div className="border p-4 rounded-lg shadow">
        <h2 className="text-xl font-semibold mb-2">📁 Ingest via CSV Upload</h2>

        {/* Hidden file input */}
        <input
          id="csvFileChooser"
          type="file"
          accept=".csv"
          style={{ display: "none" }}
          onChange={(e) => {
            const selectedFile = e.target.files[0];
            if (!selectedFile) {
              setFile(null);
              setMessage("🚫 File selection canceled.");
              return;
            }
            setFile(selectedFile);
            setMessage(`📄 Selected File: ${selectedFile.name}`);
          }}
        />

        {/* Visible Choose File Button */}
        <button
          className="bg-green-600 text-white px-4 py-2 rounded hover:bg-green-700 mt-3 mb-3"
          onClick={() => document.getElementById("csvFileChooser").click()}
        >
          Choose CSV File
        </button>

        {/* spacing added here */}
        <div className="h-4"></div>

        <button
          onClick={handleFileIngest}
          className="bg-green-600 text-white px-4 py-2 rounded hover:bg-green-700"
        >
          Upload & Ingest
        </button>
      </div>

      {/* 🟡 STATUS MESSAGE */}
      {message && (
        <p className="mt-6 text-center font-medium">{message}</p>
      )}
    </div>
  );
}
