import { Routes, Route } from "react-router-dom";
import NavBar from "./components/Navbar";
import Home from "./Pages/Home";
import SummaryVideo from "./Pages/search_summary";

function App() {
  return (
    <div className="main-content">
      <NavBar />

      {/* Page content */}
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="/summary" element={<SummaryVideo />} />
      </Routes>
    </div>
  );
}

export default App;
