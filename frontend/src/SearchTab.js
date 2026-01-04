import React, { useState } from "react";
import axios from "axios";

function SearchTab() {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState(null);
  const [loading, setLoading] = useState(false);

  const handleSearch = async () => {
    if (!query.trim()) return alert("Enter a search query.");
    try {
      setLoading(true);
      setResults(null);
      const res = await axios.post("http://127.0.0.1:8000/search", {
        query,
        n_results: 5
      });
      setResults(res.data.results);
    } catch (err) {
      setResults(null);
      alert("Error: " + err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="search-section">
      <h2>Search</h2>
      <div className="search-bar">
        <input
          className="search-input"
          placeholder="Enter query..."
          value={query}
          onChange={(e) => setQuery(e.target.value)}
        />
        <button className="search-btn" onClick={handleSearch} disabled={loading}>
          {loading ? "Searching..." : "Search"}
        </button>
      </div>

      {/* Shimmer loader */}
      {loading && (
        <div className="results-container">
          {[...Array(3)].map((_, i) => (
            <div key={i} className="video-card shimmer">
              <div className="thumbnail shimmer-box"></div>
              <div className="text shimmer-line"></div>
              <div className="text shimmer-line short"></div>
            </div>
          ))}
        </div>
      )}

      {/* Results */}
      <div className="results-container">
        {results && results.ids && results.ids[0].map((id, index) => {
          const metadata = results.metadatas[0][index];
          const thumbnailUrl = metadata.thumbnail_url
            ? metadata.thumbnail_url
            : `https://img.youtube.com/vi/${id}/hqdefault.jpg`;

          const videoUrl = `https://www.youtube.com/watch?v=${id}`;

          return (
            <div key={id} className="video-card">
              <a href={videoUrl} target="_blank" rel="noopener noreferrer" className="thumbnail-wrapper">
                <img
                  src={thumbnailUrl}
                  alt={metadata.title}
                  className="thumbnail"
                  onError={(e) => { e.target.style.display = "none"; }}
                />
                <div className="play-overlay">
                  <div className="play-icon">▶</div>
                </div>
              </a>
              <div className="video-info">
                <h3>
                  <a href={videoUrl} target="_blank" rel="noopener noreferrer">
                    {metadata.title}
                  </a>
                </h3>
                <p><strong>Channel:</strong> {metadata.channel_title}</p>
                <p><strong>Views:</strong> {metadata.view_count}</p>
                <p><strong>Duration:</strong> {metadata.duration} seconds</p>
                <p className="video-id"><strong>ID:</strong> {id}</p>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

export default SearchTab;
