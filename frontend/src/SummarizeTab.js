import React, { useState, useEffect } from "react";
import axios from "axios";

function SummarizeTab() {
  const [videoId, setVideoId] = useState("");
  const [summary, setSummary] = useState("");
  const [displayedSummary, setDisplayedSummary] = useState("");
  const [loading, setLoading] = useState(false);

  const handleSummarize = async () => {
    if (!videoId.trim()) return alert("Enter a video ID.");
    try {
      setLoading(true);
      setSummary("");
      setDisplayedSummary("");
      const res = await axios.post("http://127.0.0.1:8000/summarize", {
        video_id: videoId
      });
      setSummary(res.data.summary);
    } catch (err) {
      setSummary("Error: Could not fetch summary.");
    } finally {
      setLoading(false);
    }
  };

  // Typing animation effect
  useEffect(() => {
    if (summary) {
      let i = 0;
      const interval = setInterval(() => {
        setDisplayedSummary(summary.slice(0, i + 1));
        i++;
        if (i >= summary.length) clearInterval(interval);
      }, 20); // speed of typing
      return () => clearInterval(interval);
    }
  }, [summary]);

  return (
    <div className="summarize-section">
      <h2>Summarize Transcript</h2>
      <div className="summarize-bar">
        <input
          className="summarize-input"
          placeholder="Enter Video ID"
          value={videoId}
          onChange={(e) => setVideoId(e.target.value)}
        />
        <button
          className="summarize-btn"
          onClick={handleSummarize}
          disabled={loading}
        >
          {loading ? "Summarizing..." : "Summarize"}
        </button>
      </div>

      {displayedSummary && (
        <div className="summary-card">
          <p className="summary-title">📑 Summary</p>
          <pre className="summary-text">{displayedSummary}</pre>
        </div>
      )}
    </div>
  );
}

export default SummarizeTab;
