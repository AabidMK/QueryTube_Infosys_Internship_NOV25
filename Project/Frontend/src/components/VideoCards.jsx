import "../css/VideoCard.css";
import { useState, useEffect, useRef } from "react";
import { video_summary } from "../services/api";

function VideoCard({ ytvideo }) {
  const {
    video_id,
    title,
    thumbnail,
    view_count,
    likes,
    duration,
    channel,
  } = ytvideo;

  const [showSummary, setShowSummary] = useState(false);
  const [summary, setSummary] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const [isMinimized, setIsMinimized] = useState(false);
  const [isMaximized, setIsMaximized] = useState(false);
  const [position, setPosition] = useState({ x: 50, y: 50 });

  // TTS State
  const [isSpeaking, setIsSpeaking] = useState(false);
  const [isPaused, setIsPaused] = useState(false);
  const synthRef = useRef(window.speechSynthesis);

  // === Duration Formatter ===
  const formatDuration = (duration) => {
    if (!duration) return "0:00";

    // If already formatted like "8:24" or "1:08:24"
    if (typeof duration === "string" && duration.includes(":")) {
      return duration.trim();
    }

    let totalSeconds = 0;

    // Handle ISO 8601 duration (e.g., "PT4M13S", "PT1H8M24S")
    if (typeof duration === "string" && duration.startsWith("PT")) {
      const hours = duration.match(/(\d+)H/) ? parseInt(duration.match(/(\d+)H/)[1]) : 0;
      const minutes = duration.match(/(\d+)M/) ? parseInt(duration.match(/(\d+)M/)[1]) : 0;
      const seconds = duration.match(/(\d+)S/) ? parseInt(duration.match(/(\d+)S/)[1]) : 0;
      totalSeconds = hours * 3600 + minutes * 60 + seconds;
    } 
    // Handle raw seconds (number or string)
    else {
      totalSeconds = parseInt(duration, 10);
      if (isNaN(totalSeconds)) return "0:00";
    }

    const hrs = Math.floor(totalSeconds / 3600);
    const mins = Math.floor((totalSeconds % 3600) / 60);
    const secs = totalSeconds % 60;

    if (hrs > 0) {
      return `${hrs}:${mins.toString().padStart(2, "0")}:${secs.toString().padStart(2, "0")}`;
    }
    return `${mins}:${secs.toString().padStart(2, "0")}`;
  };

  const handleSummaryClick = async (e) => {
    e.stopPropagation();

    if (showSummary) {
      stopTTS();
      setShowSummary(false);
      return;
    }

    setShowSummary(true);
    setLoading(true);
    setError("");
    setSummary("");
    stopTTS();

    try {
      const result = await video_summary(video_id);
      if (result.status === "success") {
        setSummary(result.summary);
      } else {
        setError("Summary not available for this video.");
      }
    } catch (err) {
      setError("Failed to load summary. Please try again.");
    } finally {
      setLoading(false);
    }
  };

  const openVideo = () => {
    window.open(`https://www.youtube.com/watch?v=${video_id}`, "_blank");
  };

  // Drag handler
  const handleDragStart = (e) => {
    if (isMinimized || isMaximized) return;
    const rect = e.currentTarget.getBoundingClientRect();
    const offsetX = e.clientX - rect.left;
    const offsetY = e.clientY - rect.top;

    const handleMouseMove = (moveE) => {
      setPosition({
        x: moveE.clientX - offsetX,
        y: moveE.clientY - offsetY,
      });
    };

    const handleMouseUp = () => {
      document.removeEventListener("mousemove", handleMouseMove);
      document.removeEventListener("mouseup", handleMouseUp);
    };

    document.addEventListener("mousemove", handleMouseMove);
    document.addEventListener("mouseup", handleMouseUp);
  };

  // Text-to-Speech Functions
  const stopTTS = () => {
    synthRef.current.cancel();
    setIsSpeaking(false);
    setIsPaused(false);
  };

  const togglePlayPause = () => {
    const synth = synthRef.current;

    if (isSpeaking && !isPaused) {
      synth.pause();
      setIsPaused(true);
    } else if (isPaused) {
      synth.resume();
      setIsPaused(false);
    } else if (summary) {
      stopTTS();

      const utterance = new SpeechSynthesisUtterance(summary);
      utterance.rate = 1.1;
      utterance.pitch = 1;
      utterance.volume = 1;

      const voices = synth.getVoices();
      const goodVoice = voices.find(
        (v) =>
          v.lang.startsWith("en") &&
          (v.name.toLowerCase().includes("premium") ||
            v.name.toLowerCase().includes("neural") ||
            v.name.toLowerCase().includes("google") ||
            v.name.toLowerCase().includes("microsoft"))
      );
      if (goodVoice) utterance.voice = goodVoice;

      utterance.onend = () => {
        setIsSpeaking(false);
        setIsPaused(false);
      };
      utterance.onerror = () => {
        setIsSpeaking(false);
        setIsPaused(false);
      };

      synth.speak(utterance);
      setIsSpeaking(true);
    }
  };

  // Cleanup on unmount
  useEffect(() => {
    return () => stopTTS();
  }, []);

  return (
    <>
      <div className="video-card">
        <div className="video-thumbnail" onClick={openVideo}>
          <img src={thumbnail} alt={title} loading="lazy" />
          <span className="video-duration">
            {formatDuration(duration)}
          </span>
        </div>

        <div className="video-overlay">
          <button className="summary-button" onClick={handleSummaryClick}>
            {showSummary ? "Hide Summary" : "Summarize"}
          </button>
        </div>

        <div className="video-info">
          <h3 title={title}>{title}</h3>
          <p className="channel-name">{channel}</p>
          <p>{Number(view_count).toLocaleString()} views</p>
          <p>{Number(likes).toLocaleString()} likes</p>
        </div>
      </div>

      {/* Summary Modal */}
      {showSummary && (
        <div
          className="summary-modal-backdrop"
          onClick={() => {
            stopTTS();
            setShowSummary(false);
          }}
        >
          <div
            className={`summary-modal ${
              isMinimized ? "minimized" : ""
            } ${isMaximized ? "maximized" : ""}`}
            style={{
              left: isMaximized ? "5%" : `${position.x}px`,
              top: isMaximized ? "5%" : `${position.y}px`,
              width: isMaximized ? "90%" : "600px",
              height: isMaximized ? "90%" : "500px",
            }}
            onClick={(e) => e.stopPropagation()}
          >
            <div className="modal-header" onMouseDown={handleDragStart}>
              <h3>Summary: {title}</h3>

              <div className="modal-controls">
                {/* TTS Buttons */}
                {!loading && !error && summary && (
                  <>
                    <button
                      onClick={(e) => {
                        e.stopPropagation();
                        togglePlayPause();
                      }}
                      className="tts-header-button play-pause"
                      title={
                        isSpeaking
                          ? isPaused
                            ? "Resume"
                            : "Pause"
                          : "Read Aloud"
                      }
                    >
                      {isSpeaking ? (isPaused ? "▶" : "⏸") : "🔊"}
                    </button>
                    {(isSpeaking || isPaused) && (
                      <button
                        onClick={(e) => {
                          e.stopPropagation();
                          stopTTS();
                        }}
                        className="tts-header-button stop"
                        title="Stop"
                      >
                        ⏹
                      </button>
                    )}
                  </>
                )}

                {/* Window Controls */}
                <button
                  onClick={() => setIsMinimized(!isMinimized)}
                  title="Minimize"
                >
                  {isMinimized ? "□" : "−"}
                </button>
                <button
                  onClick={() => setIsMaximized(!isMaximized)}
                  title="Maximize"
                >
                  {isMaximized ? "❐" : "□"}
                </button>
                <button
                  onClick={() => {
                    stopTTS();
                    setShowSummary(false);
                  }}
                  title="Close"
                >
                  ×
                </button>
              </div>
            </div>

            <div className="modal-content">
              {loading ? (
                <div className="loading-container">
                  <div className="spinner"></div>
                  <p>
                    Generating AI summary...<br />
                    <small>
                      This can take 15–60 seconds for long videos
                    </small>
                  </p>
                </div>
              ) : error ? (
                <p className="error">{error}</p>
              ) : (
                <p className="summary-text">
                  {summary || "No summary available."}
                </p>
              )}
            </div>
          </div>
        </div>
      )}
    </>
  );
}

export default VideoCard;