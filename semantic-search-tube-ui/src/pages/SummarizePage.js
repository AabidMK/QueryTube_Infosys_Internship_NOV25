import { useState } from "react";
import { useNavigate } from "react-router-dom";
import axios from "axios";

export default function SummarizePage() {
  const [videoId, setVideoId] = useState("");
  const [summary, setSummary] = useState("");
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState("");

  const navigate = useNavigate();
  const API_BASE = "http://127.0.0.1:8000";

  const handleSummarize = async () => {
    if (!videoId.trim()) {
      setMessage("⚠️ Please enter a Video ID");
      return;
    }

    try {
      setLoading(true);
      setMessage("");
      setSummary("");

      const res = await axios.get(
        `${API_BASE}/summarize/${videoId}`
      );

      setSummary(res.data.summary || "No summary available");
    } catch (err) {
      console.error(err);
      setMessage("❌ Failed to generate summary");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-gray-100 p-6">
      <div className="max-w-3xl mx-auto bg-white p-6 rounded-xl shadow">

        {/* Header */}
        <h1 className="text-3xl font-bold mb-4 gradient-green-ai">
          Video Summary📝
        </h1>

        {/* Back Button */}
        <button
          onClick={() => navigate("/search")}
          className="text-blue-600 underline mb-4"
        >
          ← Back to Search
        </button>

        {/* Input + Enter key */}
        <form
          onSubmit={(e) => {
            e.preventDefault();
            handleSummarize();
          }}
          className="flex items-center mb-4"
        >
          <input
            type="text"
             className="search-input"
            value={videoId}
            onChange={(e) => setVideoId(e.target.value)}
            placeholder="Enter Video ID..."
            
          />

          <button
            type="submit"
            className="search-button"
          >
            Summarize
          </button>
        </form>

        {/* YouTube Link */}
        {videoId && (
          <a
            href={`https://www.youtube.com/watch?v=${videoId}`}
            target="_blank"
            rel="noopener noreferrer"
            className="text-red-600 font-medium underline"
          >
            ▶ Watch on YouTube
          </a>
        )}

        {/* Status */}
        {loading && (
          <p className="mt-4 text-blue-600">
            ⏳ Generating summary...
          </p>
        )}

        {message && !loading && (
          <p className="mt-4 text-red-600">{message}</p>
        )}

        {/* Summary */}
        {summary && !loading && (
          <div className="summary-box">
            <h3 className="summary-title">Summary</h3>
            <p className="summary-text">{summary}</p>
          </div>
        )}
      </div>
    </div>
  );
}
