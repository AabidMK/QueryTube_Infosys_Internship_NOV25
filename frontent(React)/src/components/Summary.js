import { useState } from "react";
import axios from "axios";

export default function Summary({ videoId }) {
  const [summary, setSummary] = useState("");
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState("");

  const getSummary = async () => {
    if (!videoId) {
      setMessage("❌ Please select a video from search.");
      return;
    }

    try {
      setLoading(true);
      setMessage("");
      setSummary("");

      const res = await axios.get(
        `http://127.0.0.1:8000/video-summary/${videoId}`
      );

      setSummary(res.data.summary || res.data.message);
    } catch (err) {
      console.error(err);
      setMessage("❌ Failed to fetch summary.");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="section-card">
      <h2>Video Summary</h2>

      <p>
        <b>Selected Video ID:</b> {videoId || "None"}
      </p>

      <div style={{ marginTop: "16px" }}>
        <button onClick={getSummary} disabled={loading}>
          {loading ? "Generating..." : "Generate Summary"}
        </button>
      </div>

      {message && <p className="message">{message}</p>}

      {summary && (
        <div
          style={{
            marginTop: "24px",
            padding: "20px",
            background: "#f3f4f6",
            borderRadius: "12px",
            textAlign: "left",
          }}
        >
          <ul style={{ paddingLeft: "20px", margin: 0 }}>
            {summary
              .split("*")
              .map((point, index) => point.trim())
              .filter((point) => point.length > 0)
              .map((point, index) => (
                <li
                  key={index}
                  style={{
                    marginBottom: "10px",
                    lineHeight: "1.6",
                    color: "#111827",
                  }}
                >
                  {point}
                </li>
              ))}
          </ul>
        </div>
      )}

    </div>
  );
}
