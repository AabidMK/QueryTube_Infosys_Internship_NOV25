from flask import Flask
from flask_cors import CORS

from app.ingest_api import ingest_bp
from app.summarisation_api import summarisation_bp
from app.search_api import search_bp

app = Flask(__name__)
CORS(app)

# Register routes
app.register_blueprint(ingest_bp)
app.register_blueprint(summarisation_bp)
app.register_blueprint(search_bp)

print("🚀 Flask app created")

if __name__ == "__main__":
    print("✅ Starting Flask server...")
    print("🌐 Server will run at http://127.0.0.1:5000")
    app.run(debug=True, port=5000)
