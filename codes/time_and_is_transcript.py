import re
from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled, NoTranscriptFound
import requests
import csv

API_KEY = "AIzaSyCe_0mCXtqQC8vbfaTDuGQsdkfkoc_rAT0"
CHANNEL_ID = "UCAuUUnT6oDeKwE6v1NGQxug"

# STEP 1: Get uploads playlist
url1 = f"https://www.googleapis.com/youtube/v3/channels?part=contentDetails&id={CHANNEL_ID}&key={API_KEY}"
data1 = requests.get(url1).json()
upload_playlist_id = data1["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

# STEP 2: Get 50 video IDs
url2 = f"https://www.googleapis.com/youtube/v3/playlistItems?part=snippet&playlistId={upload_playlist_id}&maxResults=50&key={API_KEY}"
data2 = requests.get(url2).json()
video_ids = [item["snippet"]["resourceId"]["videoId"] for item in data2["items"]]

# STEP 3: Get video full details
video_ids_string = ",".join(video_ids)
url3 = f"https://www.googleapis.com/youtube/v3/videos?part=snippet,contentDetails,statistics,status&id={video_ids_string}&key={API_KEY}"
data3 = requests.get(url3).json()

# STEP 4: Get channel details
url4 = f"https://www.googleapis.com/youtube/v3/channels?part=snippet,statistics&id={CHANNEL_ID}&key={API_KEY}"
channel_data = requests.get(url4).json()["items"][0]

channel_info = {
    "channel_title": channel_data["snippet"]["title"],
    "channel_description": channel_data["snippet"]["description"],
    "channel_country": channel_data["snippet"].get("country"),
    "channel_thumbnail": channel_data["snippet"]["thumbnails"]["high"]["url"],
    "channel_subscriberCount": channel_data["statistics"].get("subscriberCount"),
    "channel_videoCount": channel_data["statistics"].get("videoCount")
}

# Cleaning function
def clean_transcript(text):
    if not text:
        return None
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'♪[^♪]+♪', '', text)
    text = re.sub(r'\.{2,}', '.', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

# Convert duration PT#H#M#S --> seconds
def iso_to_seconds(duration):
    if not duration:
        return None

    hours = minutes = seconds = 0

    h = re.search(r"(\d+)H", duration)
    m = re.search(r"(\d+)M", duration)
    s = re.search(r"(\d+)S", duration)

    if h: hours = int(h.group(1))
    if m: minutes = int(m.group(1))
    if s: seconds = int(s.group(1))

    return hours*3600 + minutes*60 + seconds

# STEP 5: Build dataset
final_data = []

for item in data3["items"]:
    snippet = item["snippet"]
    stats = item["statistics"]
    content = item["contentDetails"]
    status = item["status"]

    # Fetch transcript
    try:
        tscript = YouTubeTranscriptApi.get_transcript(item["id"], languages=['en'])
        transcript_raw = " ".join([i["text"] for i in tscript])
        cleaned_transcript = clean_transcript(transcript_raw)
        transcript_available = True
    except:
        cleaned_transcript = None
        transcript_available = False

    video_record = {
        "id": item["id"],
        "title": snippet["title"],
        "description": snippet.get("description"),
        "publishedAt": snippet.get("publishedAt"),
        "tags": snippet.get("tags"),
        "categoryId": snippet.get("categoryId"),
        "defaultLanguage": snippet.get("defaultLanguage"),
        "defaultAudioLanguage": snippet.get("defaultAudioLanguage"),
        "thumbnail_default": snippet["thumbnails"]["default"]["url"],
        "thumbnail_high": snippet["thumbnails"]["high"]["url"],
        "duration": content.get("duration"),
        "viewCount": stats.get("viewCount"),
        "likeCount": stats.get("likeCount"),
        "commentCount": stats.get("commentCount"),
        "privacyStatus": status.get("privacyStatus"),
        "channel_id": snippet.get("channelId"),
        "channel_title": channel_info["channel_title"],
        "channel_description": channel_info["channel_description"],
        "channel_country": channel_info["channel_country"],
        "channel_thumbnail": channel_info["channel_thumbnail"],
        "channel_subscriberCount": channel_info["channel_subscriberCount"],
        "channel_videoCount": channel_info["channel_videoCount"],

        # ✅ NEW COLUMNS
        "is_transcript_available": transcript_available,
        "duration_seconds": iso_to_seconds(content.get("duration")),

        "transcript": cleaned_transcript
    }

    final_data.append(video_record)

# Save to CSV
csv_file = "ted_ed_videos_with_new_columns.csv"
csv_columns = final_data[0].keys()

with open(csv_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=csv_columns)
    writer.writeheader()
    writer.writerows(final_data)

print(f"\n🎉 File created: {csv_file}")
