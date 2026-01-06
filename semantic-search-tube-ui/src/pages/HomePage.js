export default function HomePage() {
  return (
    <div className="wholehome">

      {/* HERO SECTION */}
      <div className="max-w-4xl mx-auto text-center mb-16">
        <h1 className="text-3xl font-bold text-center mb-6 text-blue-600">
          Semantic Search Tube 🎬
        </h1>

        <p className="text-lg text-gray-700 leading-relaxed">
          Search, summarize, and explore YouTube videos intelligently using
          <span className="font-semibold"> AI-powered semantic understanding</span>.
        </p>
      </div>

      {/* FEATURES */}
      <div className="max-w-6xl mx-auto grid grid-cols-1 md:grid-cols-3 gap-8 mb-16">
        
        <div className="bg-white rounded-2xl shadow p-6 text-center hover:shadow-lg transition">
          <div className="text-4xl mb-4">📥</div>
          <h2 className="text-xl font-semibold mb-2">Ingest</h2>
          <p className="text-gray-600">
            Upload or provide CSV files containing YouTube transcripts
            to build your searchable knowledge base.
          </p>
        </div>

        <div className="bg-white rounded-2xl shadow p-6 text-center hover:shadow-lg transition">
          <div className="text-4xl mb-4">🔍</div>
          <h2 className="text-xl font-semibold mb-2">Search</h2>
          <p className="text-gray-600">
            Ask natural language questions and retrieve the most
            relevant YouTube videos using semantic search.
          </p>
        </div>

        <div className="bg-white rounded-2xl shadow p-6 text-center hover:shadow-lg transition">
          <div className="text-4xl mb-4">🧠</div>
          <h2 className="text-xl font-semibold mb-2">Summarize</h2>
          <p className="text-gray-600">
            Generate concise summaries of full YouTube videos
            using advanced AI models.
          </p>
        </div>

      </div>

      {/* CTA BUTTONS */}
      {/* <div className="flex justify-center gap-6 mb-20">
        <a
          href="/ingest"
          className="bg-blue-600 text-white px-6 py-3 m-4 rounded cursor-pointer"
        >
          Start Ingesting
        </a>

        <a
          href="/search"
          className="bg-gray-900 text-white px-8 py-3 rounded-xl text-lg font-semibold hover:bg-gray-800 transition"
        >
          Try Search
        </a>
      </div> */}

    </div>
  );
}
