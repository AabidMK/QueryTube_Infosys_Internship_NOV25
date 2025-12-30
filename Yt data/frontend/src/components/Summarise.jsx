import { useState } from "react";

export default function Summarise() {
  const [videoId, setVideoId] = useState("");
  const [summary, setSummary] = useState("");

  const summarise = async () => {
    const res = await fetch("http://localhost:5000/summarisation", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ video_id: videoId }),
    });

    const data = await res.json();
    setSummary(data.summary);
  };

  return (
    <div className="bg-zinc-800 p-6 rounded-2xl">
      <h2 className="text-xl font-semibold mb-4">📝 Video Summary</h2>

      <input
        value={videoId}
        onChange={(e) => setVideoId(e.target.value)}
        placeholder="Enter Video ID"
        className="w-full px-4 py-2 rounded-xl bg-zinc-900 border border-zinc-700 mb-4"
      />

      <button
        onClick={summarise}
        className="bg-indigo-600 px-5 py-2 rounded-xl"
      >
        Generate Summary
      </button>

      {summary && (
        <div className="mt-6 bg-zinc-900 p-4 rounded-xl border border-zinc-700 whitespace-pre-wrap">
          {summary}
        </div>
      )}
    </div>
  );
}
