import { useState } from "react";
import axios from "axios";

export default function SearchPage() {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState([]);
  const [message, setMessage] = useState("");

  const API_BASE = "http://127.0.0.1:8000";
  const formatTime = (seconds) => {
    if (seconds === undefined || seconds === null) return "00:00";
    const m = Math.floor(seconds / 60);
    const s = Math.floor(seconds % 60);
    return `${m}:${s.toString().padStart(2, "0")}`;
  };


  const handleSearch = async () => {
    if (!query.trim()) {
      setMessage("⚠️ Please enter a search query");
      return;
    }

    setMessage("🔍 Searching...");
    setResults([]);

    try {
      const res = await axios.get(`${API_BASE}/search?q=${query}`);
      const rawResults = res.data.results || [];

      // ✅ Deduplicate by video_id (keep best similarity)
      const videoMap = new Map();

      for (const item of rawResults) {
        const vid = item.video_id;
        if (!videoMap.has(vid) || item.similarity > videoMap.get(vid).similarity) {
          videoMap.set(vid, item);
        }
      }

      const uniqueResults = Array.from(videoMap.values()).slice(0, 5);

      setResults(uniqueResults);
      setMessage(uniqueResults.length ? "" : "❌ No results found");
    } catch (err) {
      console.error(err);
      setMessage("❌ Search failed. Check backend.");
    }
  };

  return (
    <div className="p-6 max-w-6xl mx-auto">

      <h1 className="text-3xl font-bold text-center mb-6 text-blue-600 gradient-search">
        Video Search🔍
      </h1>

      {/* 🔍 SEARCH BAR (unchanged layout) */}
      <form
        onSubmit={(e) => {
          e.preventDefault();
          handleSearch();
        }}
      >
        <div className="search-box">
          <input
            type="text"
            className="search-input"
            placeholder="Ask something..."
            value={query}
            onChange={(e) => setQuery(e.target.value)}
          />

          <button
            type="submit"
            className="search-button"
          >
            Search 🔍
          </button>
        </div>
      </form>

      {/* STATUS MESSAGE */}
      {message && <p className="text-center mb-4">{message}</p>}

      {/* 🎥 RESULTS (YouTube-style layout) */}
      {results.map((item, index) => {
  const thumbnailUrl = `https://img.youtube.com/vi/${item.video_id}/hqdefault.jpg`;
  const youtubeUrl = `https://www.youtube.com/watch?v=${item.video_id}`;

  return (
    <div key={index} className="result-card">

      {/* LEFT: THUMBNAIL */}
      <div className="thumbnail-wrapper">
        <img
          src={thumbnailUrl}
          alt="Video Thumbnail"
          className="video-thumbnail"
          onClick={() => window.open(youtubeUrl, "_blank")}
        />

        <span className="thumbnail-timestamp">
          {formatTime(item.start_time ?? item.timestamp ?? 0)}
        </span>
      </div>

      {/* RIGHT: DETAILS */}
      <div className="result-content">
        <h2 className="result-title">
          🎥 {item.title || "Untitled Video"}
        </h2>

        <p className="result-channel">
          <span className="label">Channel:</span>{" "}
          {item.channel_name?.trim() || "Unknown Channel"}
        </p>

        <p className="result-meta">
          <span className="label">Video ID:</span> {item.video_id}
        </p>

        <p className="result-similarity">
          Similarity: {item.similarity}
        </p>
      </div>

    </div>
  );
})}

    </div>
  );
}
