import { useState } from "react";
import axios from "axios";

/* ---------- Helpers ---------- */
const formatViews = (num) => {
  if (!num) return "0";
  if (num >= 1_000_000) return (num / 1_000_000).toFixed(1) + "M";
  if (num >= 1_000) return (num / 1_000).toFixed(1) + "K";
  return num;
};

const formatDuration = (seconds) => {
  if (!seconds && seconds !== 0) return "--:--";
  const min = Math.floor(seconds / 60);
  const sec = seconds % 60;
  return `${min}:${sec.toString().padStart(2, "0")}`;
};

export default function Search({ onSelectVideo }) {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState([]);
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState("");

  const searchVideos = async () => {
    if (!query.trim()) {
      setMessage("❌ Please enter a search query.");
      return;
    }

    try {
      setLoading(true);
      setMessage("");
      setResults([]);

      const res = await axios.get(
        `http://127.0.0.1:8000/search?query=${query}`
      );

      setResults(res.data.results);

      if (res.data.results.length === 0) {
        setMessage("⚠️ No results found.");
      }
    } catch (err) {
      console.error(err);
      setMessage("❌ Search failed. Please try again.");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="section-card">
      <h2>Video Search</h2>
      <p>Search semantically similar YouTube videos.</p>

      {/* Search Input */}
      <div className="search-box">
        <input
          type="text"
          placeholder="🔍 Search videos (AI, Tesla, McLaren...)"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && searchVideos()}
        />
      </div>

      {/* Search Button */}
      <div
        className="search-actions"
        style={{
          marginTop: "28px",
          display: "flex",
          justifyContent: "center",   // 🔑 centers the button
        }}
      >
        <button onClick={searchVideos} disabled={loading}>
          {loading ? "Searching..." : "Search"}
        </button>
      </div>


      {message && <p className="message">{message}</p>}

      {/* Results */}
      <ul style={{ listStyle: "none", padding: 0, marginTop: "28px" }}>
        {results.map((video, index) => (
          <li
            key={index}
            onClick={() => onSelectVideo(video.video_id)}
            style={{
              display: "flex",
              gap: "16px",
              background: "#f9fafb",
              padding: "14px",
              marginBottom: "18px",
              borderRadius: "12px",
              border: "1px solid #e5e7eb",
              cursor: "pointer",
              transition: "all 0.25s ease",
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.transform = "translateY(-4px)";
              e.currentTarget.style.boxShadow =
                "0 14px 30px rgba(0,0,0,0.12)";
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.transform = "translateY(0)";
              e.currentTarget.style.boxShadow = "none";
            }}
          >
            {/* Thumbnail */}
            <div
              style={{
                position: "relative",
                minWidth: "170px",
                overflow: "hidden",
                borderRadius: "10px",
              }}
            >
              <a
                href={`https://www.youtube.com/watch?v=${video.video_id}`}
                target="_blank"
                rel="noreferrer"
                onClick={(e) => e.stopPropagation()}
              >
                <img
                  src={`https://img.youtube.com/vi/${video.video_id}/hqdefault.jpg`}
                  alt="Video thumbnail"
                  width="170"
                  style={{
                    borderRadius: "10px",
                    transition: "transform 0.3s ease",
                  }}
                  onMouseEnter={(e) =>
                    (e.currentTarget.style.transform = "scale(1.06)")
                  }
                  onMouseLeave={(e) =>
                    (e.currentTarget.style.transform = "scale(1)")
                  }
                />
              </a>

              {/* Duration Badge */}
              <div
                style={{
                  position: "absolute",
                  bottom: "8px",
                  right: "8px",
                  background: "rgba(0,0,0,0.75)",
                  color: "#fff",
                  fontSize: "12px",
                  padding: "4px 6px",
                  borderRadius: "6px",
                }}
              >
                {formatDuration(video.duration_seconds)}
              </div>
            </div>

            {/* Video Info */}
            <div style={{ textAlign: "left" }}>
              <a
                href={`https://www.youtube.com/watch?v=${video.video_id}`}
                target="_blank"
                rel="noreferrer"
                onClick={(e) => e.stopPropagation()}
                style={{ textDecoration: "none", color: "#111827" }}
              >
                <h4 style={{ margin: "0 0 6px 0" }}>{video.title}</h4>
              </a>

              <p style={{ margin: 0, fontSize: "14px", color: "#4b5563" }}>
                Channel: {video.channel_title}
              </p>

              <p style={{ margin: "6px 0", fontSize: "13px", color: "#6b7280" }}>
                👁 {formatViews(video.views)} views
              </p>

              <p style={{ margin: 0, fontSize: "12px", color: "#9ca3af" }}>
                Similarity Score: {video.score.toFixed(3)}
              </p>
            </div>
          </li>
        ))}
      </ul>

      {results.length > 0 && (
        <p className="message">👉 Click a video to generate summary</p>
      )}
    </div>
  );
}
