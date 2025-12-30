from flask import Blueprint, request, jsonify
import os
import pandas as pd
from dotenv import load_dotenv
from google import genai

load_dotenv()

summarisation_bp = Blueprint("summarisation", __name__)

API_KEY = os.getenv("GEMINI_API_KEY")
CSV_PATH = "youtube_with_embeddings.csv"
MODEL = "gemini-2.5-flash"

client_gemini = genai.Client(api_key=API_KEY)

@summarisation_bp.route("/summarisation", methods=["POST"])
def summarise_video():
    data = request.json
    video_id = data.get("video_id")

    if not video_id:
        return jsonify({"error": "video_id is required"}), 400

    df = pd.read_csv(CSV_PATH)
    row = df[df["video_id"] == video_id]

    if row.empty:
        return jsonify({"error": "Video ID not found"}), 404

    transcript = row["transcript_yt"].values[0]
    if not transcript:
        return jsonify({"error": "Transcript not available"}), 400

    prompt = f"""
    Summarize the following YouTube video transcript in bullet points.
    Make it short, clear, and beginner-friendly.

    Transcript:
    {transcript}
    """

    response = client_gemini.models.generate_content(
        model=MODEL,
        contents=prompt
    )

    return jsonify({
        "video_id": video_id,
        "summary": response.text
    })
