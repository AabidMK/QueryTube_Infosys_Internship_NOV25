import { useRef } from "react";
import "../css/Navbar.css";
import { Ingestion } from "../services/api";
import { Link } from "react-router-dom";
function NavBar() {
  const fileInputRef = useRef(null);

  const handleIngestClick = () => {
    fileInputRef.current.click();
  };

  const handleFileChange = async (e) => {
    const file = e.target.files[0];
    if (!file) return;

    try {
      const result = await Ingestion(file);
      alert(`Ingested ${result.rows_ingested} rows`);
    } catch (err) {
      console.error(err);
      alert("CSV ingestion failed");
    } finally {
      e.target.value = "";
    }
  };

  return (
    <nav className="navbar">
      
        <Link to ='/' className="navbar-brand">QueryTube</Link>
        
      

      <div className="navbar-actions">
        {/* Ingest button */}
        <button className="navbar-button" onClick={handleIngestClick}>
          Ingest
        </button>

       
       
       <Link to="/summary" className="navbar-button">
          Summary
        </Link>

        <input
          type="file"
          accept=".csv"
          ref={fileInputRef}
          onChange={handleFileChange}
          style={{ display: "none" }}
        />
      </div>
    </nav>
  );
}

export default NavBar;