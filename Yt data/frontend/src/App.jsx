import { useState } from "react";
import Ingest from "./components/Ingest";
import Search from "./components/Search";
import Summarise from "./components/Summarise";

export default function App() {
  const [tab, setTab] = useState("search");

  return (
    <div className="min-h-screen p-6">
      <h1 className="text-3xl font-bold text-center mb-6">
        🎥 YouTube AI Explorer
      </h1>

      {/* Tabs */}
      <div className="flex justify-center gap-4 mb-8">
        {["ingest", "search", "summary"].map((t) => (
          <button
            key={t}
            onClick={() => setTab(t)}
            className={`px-5 py-2 rounded-xl font-medium transition ${
              tab === t
                ? "bg-indigo-600"
                : "bg-zinc-800 hover:bg-zinc-700"
            }`}
          >
            {t.toUpperCase()}
          </button>
        ))}
      </div>

      {/* Content */}
      <div className="max-w-4xl mx-auto">
        {tab === "ingest" && <Ingest />}
        {tab === "search" && <Search />}
        {tab === "summary" && <Summarise />}
      </div>
    </div>
  );
}
