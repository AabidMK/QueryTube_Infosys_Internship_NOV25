import { BrowserRouter, Routes, Route } from "react-router-dom";
import IngestPage from "./pages/ingestpage";
import SearchPage from "./pages/searchpage";
import SummaryPage from "./pages/summarypage";

export default function App() {
  return (
    <BrowserRouter>
      
      <div
        style={{
          background: "#0f172a",
          padding: "15px 25px",
          borderBottom: "1px solid #1f2937",
          display: "flex",
          gap: "20px"
        }}
      >
        <a href="/" style={{ color: "white", textDecoration: "none" }}>📦 Ingest</a>
        <a href="/search" style={{ color: "white", textDecoration: "none" }}>🔍 Search</a>
        <a href="/summary" style={{ color: "white", textDecoration: "none" }}>🧠 Summary</a>
      </div>

      <Routes>
        <Route path="/" element={<IngestPage />} />
        <Route path="/search" element={<SearchPage />} />
        <Route path="/summary" element={<SummaryPage />} />
      </Routes>
    </BrowserRouter>
  );
}
