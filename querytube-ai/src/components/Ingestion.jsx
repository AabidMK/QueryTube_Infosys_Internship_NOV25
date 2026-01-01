import { useState } from 'react';
import { Upload, CheckCircle, AlertCircle } from 'lucide-react';
import './Ingestion.css';

export default function Ingestion() {
  const [file, setFile] = useState(null);
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState(null);

  const handleFileChange = (e) => {
    const selectedFile = e.target.files[0];
    setFile(selectedFile);
    setMessage(null);
  };

  const handleUpload = async () => {
    if (!file) {
      setMessage({ type: 'error', text: 'Please select a file first.' });
      return;
    }

    const formData = new FormData();
    formData.append('file', file);

    setLoading(true);
    setMessage(null);

    try {
      const response = await fetch('http://localhost:8000/ingest', {
        method: 'POST',
        body: formData,
      });

      const data = await response.json();

      if (response.ok) {
        setMessage({
          type: 'success',
          text: `Success! ${data.rows_ingested} rows ingested.`,
        });
        setFile(null);
        // Reset file input
        document.getElementById('file-input').value = '';
      } else {
        setMessage({
          type: 'error',
          text: data.detail || 'Upload failed. Please try again.',
        });
      }
    } catch (error) {
      setMessage({
        type: 'error',
        text: 'Error connecting to server. Make sure the API is running.',
      });
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="ingestion-container">
      <h2 className="ingestion-title">Upload Documents</h2>
      <p className="ingestion-description">
        Select and upload your CSV file for ingestion into the system.
      </p>

      <div className="upload-area">
        <Upload className="upload-icon" size={48} />
        <input
          id="file-input"
          type="file"
          onChange={handleFileChange}
          accept=".csv"
          className="file-input"
        />
        {file && (
          <p className="file-name">
            Selected: <strong>{file.name}</strong>
          </p>
        )}
      </div>

      <button
        onClick={handleUpload}
        disabled={loading}
        className={`upload-button ${loading ? 'loading' : ''}`}
      >
        {loading ? 'Uploading...' : 'Upload File'}
      </button>

      {message && (
        <div className={`message ${message.type}`}>
          {message.type === 'success' ? (
            <CheckCircle size={20} />
          ) : (
            <AlertCircle size={20} />
          )}
          <span>{message.text}</span>
        </div>
      )}
    </div>
  );
}