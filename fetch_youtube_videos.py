from googleapiclient.discovery import build
import os
import pandas as pd
import isodate
from dotenv import load_dotenv

# -----------------------------
# 0. Load environment variables
# -----------------------------
load_dotenv()

API_KEY = os.getenv("API_KEY")
CHANNEL_ID = os.getenv("CHANNEL_ID")

print("CHANNEL_ID raw:", repr(CHANNEL_ID))

if not API_KEY:
    raise ValueError("❌ API_KEY is missing. Add it to your .env file as: API_KEY=your_key_here")

if not CHANNEL_ID:
    raise ValueError("❌ CHANNEL_ID is missing. Add it to your .env file as: CHANNEL_ID=UCxxxxxxxxxxxx")

print(f"Using CHANNEL_ID: {CHANNEL_ID}")

# -----------------------------
# 1. Create YouTube API client
# -----------------------------
youtube = build("youtube", "v3", developerKey=API_KEY)

# -----------------------------
# 2. Fetch CHANNEL Details
# -----------------------------
channel_res = youtube.channels().list(
    part="snippet,statistics,contentDetails",
    id=CHANNEL_ID
).execute()

# Safety check: Did we actually get a channel?
if "items" not in channel_res or len(channel_res["items"]) == 0:
    print("❌ No channel found for this CHANNEL_ID.")
    print("Raw API response:", channel_res)
    raise SystemExit("Stopping because YouTube returned 0 results for this channel.")

channel_data = channel_res["items"][0]

channel_info = {
    "channel_id": CHANNEL_ID,
    "channel_title": channel_data["snippet"]["title"],
    "channel_description": channel_data["snippet"].get("description"),
    "channel_country": channel_data["snippet"].get("country"),
    "channel_thumbnail": channel_data["snippet"]["thumbnails"]["default"]["url"],
    "channel_subscriber": channel_data["statistics"].get("subscriberCount"),
    "channel_videoCount": channel_data["statistics"].get("videoCount")
}

uploads_playlist_id = channel_data["contentDetails"]["relatedPlaylists"]["uploads"]

print(f"✅ Channel found: {channel_info['channel_title']}")
print(f"Uploads playlist ID: {uploads_playlist_id}")

# ------------------------------------
# 3. Fetch up to 50 videos from uploads
# ------------------------------------
playlist_res = youtube.playlistItems().list(
    part="snippet",
    playlistId=uploads_playlist_id,
    maxResults=50
).execute()

video_ids = []
for item in playlist_res.get("items", []):
    vid = item["snippet"]["resourceId"]["videoId"]
    video_ids.append(vid)

if not video_ids:
    raise SystemExit("❌ No videos found in the uploads playlist.")

print(f"✅ Fetched {len(video_ids)} video IDs from playlist.")

# ------------------------------------
# 4. Fetch VIDEO DETAILS + STATS
# ------------------------------------
videos_res = youtube.videos().list(
    part="snippet,contentDetails,statistics,status",
    id=",".join(video_ids)
).execute()

final_data = []

for v in videos_res.get("items", []):
    snippet = v["snippet"]
    stats = v.get("statistics", {})
    content = v["contentDetails"]
    status = v.get("status", {})

    # Parse duration (ISO → seconds)
    duration_iso = content.get("duration")
    duration_seconds = None
    if duration_iso:
        try:
            duration_seconds = int(isodate.parse_duration(duration_iso).total_seconds())
        except Exception:
            duration_seconds = None

    video_record = {
        "id": v["id"],
        "title": snippet.get("title"),
        "description": snippet.get("description"),
        "publishedAt": snippet.get("publishedAt"),
        "tags": ",".join(snippet.get("tags", [])),
        "categoryId": snippet.get("categoryId"),
        "defaultLanguage": snippet.get("defaultLanguage"),
        "defaultAudioLanguage": snippet.get("defaultAudioLanguage"),

        "thumbnail_default": snippet["thumbnails"]["default"]["url"],
        "thumbnail_high": snippet["thumbnails"]["high"]["url"],

        "duration": duration_seconds,
        "privacyStatus": status.get("privacyStatus"),

        "viewCount": stats.get("viewCount"),
        "likeCount": stats.get("likeCount"),
        "commentCount": stats.get("commentCount"),

        # Add channel info (duplicated per row for convenience)
        "channel_id": channel_info["channel_id"],
        "channel_title": channel_info["channel_title"],
        "channel_description": channel_info["channel_description"],
        "channel_country": channel_info["channel_country"],
        "channel_thumbnail": channel_info["channel_thumbnail"],
        "channel_subscriber": channel_info["channel_subscriber"],
        "channel_videoCount": channel_info["channel_videoCount"]
    }

    final_data.append(video_record)

print(f"✅ Collected details for {len(final_data)} videos.")

# ------------------------------------
# 5. Save to CSV
# ------------------------------------
output_csv = "youtube_50_videos.csv"
df = pd.DataFrame(final_data)
df.to_csv(output_csv, index=False, encoding="utf-8")

print(f"\n🎉 CSV saved successfully: {output_csv}")
