import requests
import csv

API_KEY = "AIzaSyCe_0mCXtqQC8vbfaTDuGQsdkfkoc_rAT0"
CHANNEL_ID = "UCAuUUnT6oDeKwE6v1NGQxug"  # TED-Ed

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

# STEP 4: Get channel details (one call only)
url4 = f"https://www.googleapis.com/youtube/v3/channels?part=snippet,statistics&id={CHANNEL_ID}&key={API_KEY}"
channel_data = requests.get(url4).json()["items"][0]

# Extract channel info
channel_info = {
    "channel_title": channel_data["snippet"]["title"],
    "channel_description": channel_data["snippet"]["description"],
    "channel_country": channel_data["snippet"].get("country"),
    "channel_thumbnail": channel_data["snippet"]["thumbnails"]["high"]["url"],
    "channel_subscriberCount": channel_data["statistics"].get("subscriberCount"),
    "channel_videoCount": channel_data["statistics"].get("videoCount")
}

# STEP 5: Merge video + channel data
final_data = []

for item in data3["items"]:
    snippet = item["snippet"]
    stats = item["statistics"]
    content = item["contentDetails"]
    status = item["status"]

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
        "channel_videoCount": channel_info["channel_videoCount"]
    }

    final_data.append(video_record)

# STEP 6: Save to CSV
csv_file = "ted_videos.csv"
csv_columns = final_data[0].keys()

with open(csv_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=csv_columns)
    writer.writeheader()
    writer.writerows(final_data)

print(f"\nCSV file '{csv_file}' created successfully!")
