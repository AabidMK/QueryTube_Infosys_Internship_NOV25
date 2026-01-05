const API_BASE = "http://localhost:8000";

export const SearchVideos=async(query,top_k=5)=>{
  const response=await fetch(
    `${API_BASE}/search?query=${encodeURIComponent(query)}&top_k=${top_k}`
  );
  if(!response.ok){throw new Error("Search request failed");
  
  }
 return response.json();
}

export const video_summary=async (video_id)=>{
  const response =await fetch(
    `${API_BASE}/summary?video_id=${encodeURIComponent(video_id)}`
  );
  if(!response.ok){throw new Error("Summary request failed");  
  }
  return response.json();
  }
//to use a file we use form data
export const Ingestion = async (file) => {
  const formData = new FormData();     // ✅ do NOT overwrite FormData
  formData.append("file", file);       // ✅ append correctly

  const response = await fetch(`${API_BASE}/ingest-csv`, {
    method: "POST",                    // ❌ methode → ✅ method
    body: formData,
  });

  if (!response.ok) {
    throw new Error("ingest request failed");
  }

  return response.json();
};