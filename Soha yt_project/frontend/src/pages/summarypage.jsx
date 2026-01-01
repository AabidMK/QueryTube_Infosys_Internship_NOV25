import { useEffect, useState } from "react";
import API from "../api/api";

export default function SummaryPage() {

  const urlParams = new URLSearchParams(window.location.search);
  const preId = urlParams.get("video");

  const [videoId, setVideoId] = useState(preId || "");
  const [data, setData] = useState(null);

  const loadSummary = async () => {
    if (!videoId.trim()) return;

    setData("loading");

    const res = await API.get("/summary", {
      params: { video_id: videoId }
    });

    setData(res.data);
  };

  useEffect(() => {
    if (preId) loadSummary();
  }, []);

  // Converts text bullets into <li>
  const renderBullets = (text) => {
    const lines = text
      .split("\n")
      .map(l => l.replace(/^[-•\s]+/, "").trim())
      .filter(l => l);

    return (
      <ul style={{ marginTop: 10, lineHeight: "28px" }}>
        {lines.map((line, i) => (
          <li key={i}>{line}</li>
        ))}
      </ul>
    );
  };

  return (
    <div className="container" style={{ paddingTop: 60 }}>
      
      <h1 style={{ fontSize: 38, fontWeight: 800 }}>
        🧠 AI Video Summary
      </h1>

      <p style={{ color: "#9ca3af", marginTop: 8 }}>
        Enter a video ID from search results to generate a clean bullet summary
      </p>

      <div style={{ display: "flex", gap: 10, marginTop: 20 }}>
        <input
          className="input"
          value={videoId}
          onChange={(e) => setVideoId(e.target.value)}
          placeholder="Enter YouTube Video ID"
        />

        <button
          className="button"
          style={{ background: "#22c55e" }}
          onClick={loadSummary}
        >
          Generate
        </button>
      </div>

      {/* Loading State */}
      {data === "loading" && (
        <p style={{ marginTop: 25, fontSize: 18 }}>
          ⏳ Generating summary... Please wait
        </p>
      )}

      {/* Summary Card */}
      {data && data !== "loading" && (
        <div style={{
          marginTop: 30,
          background: "#020617",
          padding: 20,
          borderRadius: 12,
          border: "1px solid #1f2937"
        }}>
          
          <img
            src={data.thumbnail}
            width="350"
            style={{ borderRadius: 12 }}
          />

          <h2 style={{ marginTop: 10 }}>{data.title}</h2>

          <p style={{ color: "#9ca3af", marginTop: 5 }}>
            Views: {data.views} • Duration: {data.duration}
          </p>

          <h3 style={{ marginTop: 15 }}>Summary</h3>

          <div style={{ marginTop: 5, fontSize: 16 }}>
            {renderBullets(data.summary)}
          </div>
        </div>
      )}
    </div>
  );
}
