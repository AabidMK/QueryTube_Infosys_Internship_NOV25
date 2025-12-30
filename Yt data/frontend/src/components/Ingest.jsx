import { useState } from "react";

export default function Ingest() {
  const [file, setFile] = useState(null);
  const [message, setMessage] = useState("");

  const uploadCSV = async () => {
    if (!file) return;

    const formData = new FormData();
    formData.append("file", file);

    const res = await fetch("http://localhost:5000/ingest", {
      method: "POST",
      body: formData,
    });

    const data = await res.json();
    setMessage(`✅ ${data.message} (${data.records_inserted} records)`);
  };

  return (
    <div className="bg-zinc-800 p-6 rounded-2xl">
      <h2 className="text-xl font-semibold mb-4">📤 Ingest CSV</h2>

      <input
        type="file"
        accept=".csv"
        onChange={(e) => setFile(e.target.files[0])}
        className="mb-4"
      />

      <button
        onClick={uploadCSV}
        className="bg-indigo-600 px-4 py-2 rounded-lg"
      >
        Upload & Ingest
      </button>

      {message && (
        <p className="mt-4 text-green-400 font-medium">{message}</p>
      )}
    </div>
  );
}
