import React, { useState } from "react";
import { video_summary } from "../services/api";

function SummaryVideo() {
  const [videoId, setVideoId] = useState("");
  const [summary, setSummary] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const handleSummary = async () => {
    if (!videoId.trim()) {
      setError("Please enter a video ID");
      return;
    }

    setLoading(true);
    setError("");
    setSummary("");

    try {
      const result = await video_summary(videoId);
      setSummary(result.summary || "No summary returned.");
    } catch (err) {
      setError("Failed to fetch summary. Please check the video ID and try again.");
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div
      style={{
        maxWidth: "700px",
        margin: "3rem auto",
        padding: "2.5rem",
        background: "linear-gradient(135deg, #140a2a, #1d0f38)",
        borderRadius: "20px",
        boxShadow: "0 20px 60px rgba(0,0,0,0.45)",
        color: "#ffffff",
      }}
    >
      <h2 style={{ marginBottom: "2rem", textAlign: "center" }}>
        🎬 Video Summary Generator
      </h2>

      {/* INPUT */}
      <div style={{ marginBottom: "1.5rem" }}>
        <label style={{ fontWeight: 600 }}>Enter Video ID</label>

        <input
          type="text"
          value={videoId}
          onChange={(e) => setVideoId(e.target.value)}
          placeholder="e.g. abc123xyz"
          disabled={loading}
          style={{
            width: "100%",
            marginTop: "0.6rem",
            padding: "0.9rem 1.2rem",
            fontSize: "1rem",
            borderRadius: "12px",
            border: "2px solid #6a1b9a",
            backgroundColor: "#1a0b2e",
            color: "#ffffff",
            outline: "none",
            transition: "all 0.25s ease",
          }}
          onFocus={(e) =>
            (e.target.style.boxShadow =
              "0 0 0 3px rgba(156, 39, 176, 0.45)")
          }
          onBlur={(e) => (e.target.style.boxShadow = "none")}
        />
      </div>

      {/* BUTTON */}
      <button
        onClick={handleSummary}
        disabled={loading || !videoId.trim()}
        style={{
          width: "100%",
          padding: "0.9rem",
          fontSize: "1.05rem",
          fontWeight: 600,
          borderRadius: "50px",
          border: "none",
          cursor: loading ? "not-allowed" : "pointer",
          background: loading
            ? "#555"
            : "linear-gradient(to right, #9c27b0, #7b1fa2)",
          color: "#ffffff",
          boxShadow: "0 8px 25px rgba(156, 39, 176, 0.45)",
          transition: "transform 0.2s ease, box-shadow 0.2s ease",
        }}
        onMouseEnter={(e) =>
          (e.currentTarget.style.transform = "translateY(-2px)")
        }
        onMouseLeave={(e) =>
          (e.currentTarget.style.transform = "translateY(0)")
        }
      >
        {loading ? "Generating Summary..." : "Get Summary"}
      </button>

      {/* ERROR */}
      {error && (
        <p
          style={{
            marginTop: "1.5rem",
            color: "#ff6b6b",
            textAlign: "center",
            fontWeight: 500,
          }}
        >
          {error}
        </p>
      )}

      {/* SUMMARY */}
      {summary && (
        <div style={{ marginTop: "2.5rem" }}>
          <label style={{ fontWeight: 600 }}>Summary</label>

          <textarea
            value={summary}
            readOnly
            rows={10}
            style={{
              width: "100%",
              marginTop: "0.6rem",
              padding: "1rem",
              fontSize: "1rem",
              borderRadius: "14px",
              backgroundColor: "#1a0b2e",
              color: "#e9ddff",
              border: "2px solid #6a1b9a",
              resize: "vertical",
              lineHeight: "1.6",
            }}
          />
        </div>
      )}
    </div>
  );
}

export default SummaryVideo;
