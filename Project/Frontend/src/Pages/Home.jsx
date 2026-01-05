import VideoCard from "../components/VideoCards"; // 
import { useState, useEffect, useRef } from "react";
import '../css/Home.css';
import { SearchVideos } from '../services/api';

function Home() {
    const [searchQuery, setSearchQuery] = useState("");
    const [videos, setVideos] = useState([]);
    const [error, setError] = useState(null);
    const [loading, setLoading] = useState(true);

    // Speech Recognition
    const [isListening, setIsListening] = useState(false);
    const recognitionRef = useRef(null);

    useEffect(() => {
        // Load default videos on mount
        const loadSomeVideos = async () => {
            try {
                const data = await SearchVideos('machine learning', 6);
                setVideos(data.results);
            } catch (err) {
                console.log(err);
                setError('Failed to load videos');
            } finally {
                setLoading(false);
            }
        };
        loadSomeVideos();

        // Setup Speech Recognition (only if supported)
        if ('SpeechRecognition' in window || 'webkitSpeechRecognition' in window) {
            const SpeechRecognition = window.SpeechRecognition || window.webkitSpeechRecognition;
            const recognition = new SpeechRecognition();
            recognition.continuous = false;
            recognition.interimResults = true;
            recognition.lang = 'en-US';

            recognition.onresult = (event) => {
                const transcript = Array.from(event.results)
                    .map(result => result[0])
                    .map(result => result.transcript)
                    .join('');

                setSearchQuery(transcript);

                // If speech is final, trigger search
                if (event.results[0].isFinal) {
                    setIsListening(false);
                    if (transcript.trim()) {
                        handleSearch(new Event('submit')); // Trigger search
                    }
                }
            };

            recognition.onerror = (event) => {
                console.error("Speech recognition error:", event.error);
                setIsListening(false);
                if (event.error === 'not-allowed') {
                    alert("Microphone access denied. Please allow microphone permission.");
                }
            };

            recognition.onend = () => {
                setIsListening(false);
            };

            recognitionRef.current = recognition;
        }

        // Cleanup on unmount
        return () => {
            if (recognitionRef.current) {
                recognitionRef.current.abort();
            }
        };
    }, []);

    const handleSearch = async (e) => {
        e?.preventDefault();
        if (!searchQuery.trim() || loading) return;

        setLoading(true);
        setError(null);

        try {
            const searchResults = await SearchVideos(searchQuery, 6);
            setVideos(searchResults.results);
        } catch (err) {
            console.log(err);
            setError('Failed to search videos');
        } finally {
            setLoading(false);
        }
    };

    const toggleVoiceSearch = () => {
        if (!recognitionRef.current) {
            alert("Speech recognition not supported in your browser. Try Chrome or Edge.");
            return;
        }

        if (isListening) {
            recognitionRef.current.stop();
            setIsListening(false);
        } else {
            recognitionRef.current.start();
            setIsListening(true);
        }
    };

    return (
        <div className="home">
            <form onSubmit={handleSearch} className="search-form">
                <div className="search-input-container">
                    <input
                        type="text"
                        placeholder="Search for videos... (or click mic to speak)"
                        className="search-input"
                        value={searchQuery}
                        onChange={(e) => setSearchQuery(e.target.value)}
                    />
                    <button
                        type="button"
                        className={`voice-search-button ${isListening ? 'listening' : ''}`}
                        onClick={toggleVoiceSearch}
                        title="Voice Search"
                        aria-label="Voice search"
                    >
                        {isListening ? '🎙️' : '🎤'}
                    </button>
                </div>
                <button type="submit" className="search-button">
                    Search
                </button>
            </form>

            {error && <div className="error-message">{error}</div>}

            {loading ? (
                <div className="loading">Loading videos...</div>
            ) : (
                <div className="videos-grid">
                    {videos.map(video => (
                        <VideoCard ytvideo={video} key={video.video_id} />
                    ))}
                </div>
            )}
        </div>
    );
}

export default Home;