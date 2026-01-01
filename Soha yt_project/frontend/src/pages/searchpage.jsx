import { useState } from "react";
import { useNavigate } from "react-router-dom";
import API from "../api/api";

export default function SearchPage() {

  const [query, setQuery] = useState("");
  const [results, setResults] = useState([]);
  const navigate = useNavigate();

  const handleSearch = async () => {
    if (!query.trim()) return;

    const res = await API.get("/search", {
      params: { query }
    });

    setResults(res.data.results);
  };

  return (
    <div className="container" style={{ paddingTop: 60 }}>
      
      <h1 style={{ fontSize: 38, fontWeight: 800 }}>
        🔍 Smart YouTube AI Search
      </h1>

      <p style={{ color: "#9ca3af", marginTop: 8 }}>
        Search videos intelligently using semantic meaning
      </p>

      <div style={{
        marginTop: 25,
        display: "flex",
        gap: 10
      }}>
        <input
          className="input"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="Search anything..."
        />

        <button
          onClick={handleSearch}
          className="button"
          style={{ background: "#38bdf8" }}
        >
          Search
        </button>
      </div>

      <div style={{
        marginTop: 40,
        display: "grid",
        gridTemplateColumns: "repeat(auto-fit, minmax(300px,1fr))",
        gap: 20
      }}>
        {results.map((v, i) => (
          <div
            key={i}
            style={{
              background: "#020617",
              padding: 12,
              borderRadius: 10,
              border: "1px solid #1f2937"
            }}
          >
            <img src={v.thumbnail} style={{ width: "100%", borderRadius: 10 }} />

            <h3 style={{ marginTop: 10 }}>{v.title}</h3>

            <p style={{ color: "#9ca3af", fontSize: 13 }}>
              Video ID: {v.video_id}
            </p>

            <p style={{ color: "#9ca3af", fontSize: 14 }}>
              Views: {v.views}
            </p>

            <div style={{
              display: "flex",
              justifyContent: "space-between",
              marginTop: 10
            }}>
              <button
                className="button"
                style={{ background: "#ef4444" }}
                onClick={() => window.open(`https://youtu.be/${v.video_id}`)}
              >
                Watch
              </button>

              <button
                className="button"
                style={{ background: "#22c55e" }}
                onClick={() => navigate(`/summary?video=${v.video_id}`)}
              >
                Summary
              </button>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
