import './App.css';
import FloatingBackground from "./FloatingBackground";
import React, { useState } from "react";
import IngestTab from "./IngestTab";
import SearchTab from "./SearchTab";
import SummarizeTab from "./SummarizeTab";

function App() {
  const [activeTab, setActiveTab] = useState("ingest");

  return (
    <div className="app">
      <FloatingBackground />

      <div className="header">
        <h1>Video Transcript System</h1>
        <nav>
          <button
            className={activeTab === "ingest" ? "active" : ""}
            onClick={() => setActiveTab("ingest")}
          >
            Ingest
          </button>
          <button
            className={activeTab === "search" ? "active" : ""}
            onClick={() => setActiveTab("search")}
          >
            Search
          </button>
          <button
            className={activeTab === "summarize" ? "active" : ""}
            onClick={() => setActiveTab("summarize")}
          >
            Summarize
          </button>
        </nav>
      </div>

      <div className="tab-content">
        {activeTab === "ingest" && <IngestTab />}
        {activeTab === "search" && <SearchTab />}
        {activeTab === "summarize" && <SummarizeTab />}
      </div>
    </div>
  );
}

export default App;

// // uvicorn app:app --reload --port 8000

