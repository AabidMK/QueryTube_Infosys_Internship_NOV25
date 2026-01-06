import "./App.css";
import { BrowserRouter as Router, Routes, Route, Link } from "react-router-dom";
import Home from "./pages/HomePage";
import IngestPage from "./pages/IngestPage";
import SearchPage from "./pages/searchpage";
import SummarizePage from "./pages/SummarizePage";

export default function App() {
  return (
    <Router>
      {/* HEADER */}
      <header className="app-header">
        <div className="app-title">
          AI-Semantic Search Tube
        </div>

        <nav className="nav-links">
          <Link to="/">🏠Home</Link>
          <Link to="/ingest">📥Ingest</Link>
          <Link to="/search">🔍Search</Link>
          <Link to="/summarize">📝Summarize</Link>
        </nav>
      </header>

      {/* PAGES */}
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="/ingest" element={<IngestPage />} />
        <Route path="/search" element={<SearchPage />} />
        <Route path="/summarize" element={<SummarizePage />} />
      </Routes>
    </Router>
  );
}
