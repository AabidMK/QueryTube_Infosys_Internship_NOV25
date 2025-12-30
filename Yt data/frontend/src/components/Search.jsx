import { useState } from "react";

export default function Search() {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState([]);

  const search = async () => {
    const res = await fetch(
      `http://localhost:5000/search?query=${encodeURIComponent(query)}`
    );
    const data = await res.json();
    setResults(data);
  };

  return (
    <div className="bg-zinc-800 p-6 rounded-2xl">
      <h2 className="text-xl font-semibold mb-4">🔍 Semantic Search</h2>

      <div className="flex gap-3 mb-6">
        <input
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="Search YouTube content..."
          className="flex-1 px-4 py-2 rounded-xl bg-zinc-900 border border-zinc-700"
        />
        <button
          onClick={search}
          className="bg-indigo-600 px-5 py-2 rounded-xl"
        >
          Search
        </button>
      </div>

      <div className="space-y-4">
        {results.map((r, i) => (
          <div
            key={i}
            className="p-4 bg-zinc-900 rounded-xl border border-zinc-700"
          >
            <p className="font-semibold">{r.title}</p>
            <p className="text-sm text-zinc-400">{r.channel_name}</p>
            <p className="text-xs text-zinc-500">
              Video ID: {r.video_id}
            </p>
            <p className="text-xs mt-1">
              Similarity: <span className="text-indigo-400">{r.similarity}</span>
            </p>
          </div>
        ))}
      </div>
    </div>
  );
}
