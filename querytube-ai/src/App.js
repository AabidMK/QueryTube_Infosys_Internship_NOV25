import { useState } from 'react';
import { Upload, Search, FileText } from 'lucide-react';
import Ingestion from './components/Ingestion';
import SearchQuery from './components/SearchQuery';
import Summary from './components/Summary';
import './App.css';

export default function App() {
  const [activeTab, setActiveTab] = useState('ingestion');

  const tabs = [
    { id: 'ingestion', label: 'File Ingestion', icon: Upload },
    { id: 'search', label: 'Search Query', icon: Search },
    { id: 'summary', label: 'Get Summary', icon: FileText },
  ];

  return (
    <div className="app-container">
      <div className="app-content">
        <h1 className="app-title">🎥 queryTube AI</h1>

        {/* Tabs */}
        <div className="tabs-container">
          {tabs.map((tab) => {
            const Icon = tab.icon;
            return (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                className={`tab-button ${activeTab === tab.id ? 'active' : ''}`}
              >
                <Icon size={20} />
                {tab.label}
              </button>
            );
          })}
        </div>

        {/* Content Area */}
        <div className="content-card">
          {activeTab === 'ingestion' && <Ingestion />}
          {activeTab === 'search' && <SearchQuery />}
          {activeTab === 'summary' && <Summary />}
        </div>
      </div>
    </div>
  );
}