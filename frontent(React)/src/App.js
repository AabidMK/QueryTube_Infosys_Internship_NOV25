import React, { useState } from "react";
import Ingestion from "./components/Ingestion";
import Search from "./components/Search";
import Summary from "./components/Summary";

function App() {
  const [activeTab, setActiveTab] = useState("ingest");
  const [selectedVideoId, setSelectedVideoId] = useState("");

  return (
    <div
      style={{
        minHeight: "100vh",
        background: "linear-gradient(135deg, #eef2ff, #f8fafc)",
        padding: "40px 16px",
      }}
    >
      {/* CENTERED APP CONTAINER (KEY PART) */}
      <div
        style={{
          maxWidth: "1050px",   // 🔑 THIS is what matches the reference UI
          margin: "0 auto",
        }}
      >
        {/* HEADER */}
        <h1
          style={{
            textAlign: "center",
            marginBottom: "28px",
          }}
        >
          AI-QueryTube
        </h1>

        {/* TABS (SAME WIDTH AS CONTENT) */}
        <div
          style={{
            display: "flex",
            gap: "16px",
            justifyContent: "center",
            marginBottom: "32px",
          }}
        >
          <button
            className={`tab-btn ${activeTab === "ingest" ? "active" : ""}`}
            onClick={() => setActiveTab("ingest")}
          >
            INGEST
          </button>

          <button
            className={`tab-btn ${activeTab === "search" ? "active" : ""}`}
            onClick={() => setActiveTab("search")}
          >
            SEARCH
          </button>

          <button
            className={`tab-btn ${activeTab === "summary" ? "active" : ""}`}
            onClick={() => setActiveTab("summary")}
          >
            SUMMARY
          </button>
        </div>

        {/* CONTENT CARD */}
       
          {activeTab === "ingest" && <Ingestion />}

          {activeTab === "search" && (
            <Search
              onSelectVideo={(id) => {
                setSelectedVideoId(id);
                setActiveTab("summary");
              }}
            />
          )}

          {activeTab === "summary" && (
            <Summary videoId={selectedVideoId} />
          )}
        </div>
      </div>
  );
}

export default App;
