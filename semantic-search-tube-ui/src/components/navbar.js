import { Link, useLocation } from "react-router-dom";

export default function Navbar() {
  const location = useLocation();

  const linkClass = (path) =>
    `px-4 py-2 rounded ${
      location.pathname === path
        ? "bg-blue-600 text-white"
        : "text-gray-700 hover:bg-gray-200"
    }`;

  return (
    <nav className="bg-white shadow mb-6">
      <div className="max-w-6xl mx-auto px-6 py-3 flex justify-between items-center">
        
        {/* App Name */}
        <h1 className="text-xl font-bold text-blue-600">
          Semantic Search Tube
        </h1>

        {/* Links */}
        <div className="flex gap-3">
          <Link to="/search" className={linkClass("/search")}>
            Search
          </Link>

          <Link to="/ingest" className={linkClass("/ingest")}>
            Ingest
          </Link>

          <Link to="/summarize" className={linkClass("/summarize")}>
            Summarize
          </Link>
        </div>
      </div>
    </nav>
  );
}
