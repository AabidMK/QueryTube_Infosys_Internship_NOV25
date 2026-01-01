import { useState } from "react";
import { Search, AlertCircle } from "lucide-react";
import "./SearchQuery.css";

export default function SearchQuery() {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const handleSearch = async () => {
    if (!query.trim()) {
      setError("Please enter a search query.");
      return;
    }

    setLoading(true);
    setError(null);
    setResults(null);

    try {
      const response = await fetch(
        `http://localhost:8000/search?query=${encodeURIComponent(query)}`
      );

      const data = await response.json();

      if (response.ok) {
        setResults(data);
      } else {
        setError(data.detail || "Search failed. Please try again.");
      }
    } catch (err) {
      setError("Error connecting to server. Make sure the API is running.");
    } finally {
      setLoading(false);
    }
  };

  const handleKeyPress = (e) => {
    if (e.key === "Enter") {
      handleSearch();
    }
  };

  return (
    <div className="search-container">
      <h2 className="search-title">Search Documents</h2>
      <p className="search-description">
        Enter your query to perform semantic search through uploaded documents.
      </p>

      <div className="search-bar">
        <input
          type="text"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onKeyPress={handleKeyPress}
          placeholder="Enter search query..."
          className="search-input"
        />
        <button
          onClick={handleSearch}
          disabled={loading}
          className={`search-button ${loading ? "loading" : ""}`}
        >
          <Search size={20} />
          {loading ? "Searching..." : "Search"}
        </button>
      </div>

      {error && (
        <div className="error-message">
          <AlertCircle size={20} />
          <span>{error}</span>
        </div>
      )}

      {results && (
        <div className="results-container">
          <h3 className="results-title">Search Results:</h3>
          <div className="results-content">
            {Array.isArray(results) ? (
              results.length > 0 ? (
                results.map((result, index) => (
                  <div key={index} className="result-item">
                    <div className="result-header">
                      <span className="result-number">Result {index + 1}</span>
                      {result.score && (
                        <span className="result-score">
                          Score: {result.score.toFixed(3)}
                        </span>
                      )}
                    </div>
                    <div className="result-content">
                      {result.video_id && (
                        <a
                          href={`https://www.youtube.com/watch?v=${result.video_id}`}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="thumbnail-link"
                        >
                          <img
                            src={`https://img.youtube.com/vi/${result.video_id}/hqdefault.jpg`}
                            alt={result.title || "YouTube thumbnail"}
                            className="youtube-thumbnail"
                          />
                        </a>
                      )}

                      <div className="result-details">
                        <h4 className="video-title">
                          <a
                            href={`https://www.youtube.com/watch?v=${result.video_id}`}
                            target="_blank"
                            rel="noopener noreferrer"
                          >
                            {result.title || "Untitled Video"}
                          </a>
                        </h4>

                        <p className="channel-name">
                          Channel: {result.channel_title || "Unknown"}
                        </p>
                        <p>Video_Id: {result.video_id}</p>

                        {result.similarity_score !== undefined && (
                          <p className="similarity-score">
                            Similarity: {result.similarity_score.toFixed(3)}
                          </p>
                        )}
                      </div>
                    </div>
                  </div>
                ))
              ) : (
                <p className="no-results">No results found for your query.</p>
              )
            ) : (
              <pre className="results-raw">
                {JSON.stringify(results, null, 2)}
              </pre>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
