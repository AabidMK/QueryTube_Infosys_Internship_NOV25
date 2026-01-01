import { useState } from 'react';
import { FileText, AlertCircle } from 'lucide-react';
import './Summary.css';

export default function Summary() {
  const [videoId, setVideoId] = useState('');
  const [summary, setSummary] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const handleGetSummary = async () => {
    if (!videoId.trim()) {
      setError('Please enter a video ID.');
      return;
    }

    setLoading(true);
    setError(null);
    setSummary(null);

    try {
      const response = await fetch(
        `http://localhost:8000/summary/${encodeURIComponent(videoId)}`
      );

      const data = await response.json();

      if (response.ok) {
        setSummary(data);
      } else {
        setError(data.detail || 'Failed to get summary. Please try again.');
      }
    } catch (err) {
      setError('Error connecting to server. Make sure the API is running.');
    } finally {
      setLoading(false);
    }
  };

  const handleKeyPress = (e) => {
    if (e.key === 'Enter') {
      handleGetSummary();
    }
  };

  return (
    <div className="summary-container">
      <h2 className="summary-title">Get Video Summary</h2>
      <p className="summary-description">
        Enter a video ID to generate and retrieve its summary.
      </p>

      <div className="input-group">
        <input
          type="text"
          value={videoId}
          onChange={(e) => setVideoId(e.target.value)}
          onKeyPress={handleKeyPress}
          placeholder="Enter video ID..."
          className="video-input"
        />
        <button
          onClick={handleGetSummary}
          disabled={loading}
          className={`summary-button ${loading ? 'loading' : ''}`}
        >
          <FileText size={20} />
          {loading ? 'Loading...' : 'Generate Summary'}
        </button>
      </div>

      {error && (
        <div className="error-message">
          <AlertCircle size={20} />
          <span>{error}</span>
        </div>
      )}

      {summary && (
        <div className="summary-result">
          <div className="summary-header">
            <h3 className="summary-result-title">Summary</h3>
            {summary.video_id && (
              <span className="video-id-badge">Video ID: {summary.video_id}</span>
            )}
          </div>
          <div className="summary-content">
            {typeof summary.summary === 'string' ? (
              <p className="summary-text">{summary.summary}</p>
            ) : (
              <pre className="summary-json">
                {JSON.stringify(summary, null, 2)}
              </pre>
            )}
          </div>
        </div>
      )}
    </div>
  );
}