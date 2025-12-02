import requests
import csv

# Configuration
API_KEY = "api_key"  
CHANNEL_ID = "UClcE-kVhqyiHCcjYwcpfj9w"  
OUTPUT_FILE = "live_overflow_videos.csv"
TARGET_VIDEOS = 50  

# Get channel info
print("Getting channel data...")
url = "https://www.googleapis.com/youtube/v3/channels"
params = {"part": "snippet,statistics,contentDetails", "id": CHANNEL_ID, "key": API_KEY}
response = requests.get(url, params=params).json()

if "items" not in response or len(response["items"]) == 0:
    print(f" Channel not found: {CHANNEL_ID}")
    exit()

channel = response["items"][0]
uploads_id = channel["contentDetails"]["relatedPlaylists"]["uploads"]

channel_info = {
    "channel_id": CHANNEL_ID,
    "channel_title": channel["snippet"]["title"],
    "channel_description": channel["snippet"].get("description", ""),
    "channel_country": channel["snippet"].get("country", ""),
    "channel_thumbnail": channel["snippet"]["thumbnails"]["high"]["url"],
    "channel_subscriberCount": channel["statistics"].get("subscriberCount", "0"),
    "channel_videoCount": channel["statistics"].get("videoCount", "0")
}

print(f" Channel: {channel_info['channel_title']}")

print(f"\nFetching video IDs from uploads playlist...")
all_video_ids = []
next_page_token = None
max_to_fetch = TARGET_VIDEOS + 20  # Get extra to account for unavailable videos

while len(all_video_ids) < max_to_fetch:
    url = "https://www.googleapis.com/youtube/v3/playlistItems"
    params = {
        "part": "contentDetails",
        "playlistId": uploads_id,
        "maxResults": min(50, max_to_fetch - len(all_video_ids)),
        "pageToken": next_page_token,
        "key": API_KEY
    }
    response = requests.get(url, params=params).json()
    
    if "items" not in response:
        print(" No videos found or error in response")
        break
    
    for item in response["items"]:
        all_video_ids.append(item["contentDetails"]["videoId"])
    
    next_page_token = response.get("nextPageToken")
    if not next_page_token:
        break  # No more videos

print(f"Retrieved {len(all_video_ids)} video IDs from playlist")

#  Fetch details in batches and filter out unavailable videos
print("\nFetching video details (filtering unavailable videos)...")
all_video_data = []
processed_ids = []

# Process in batches of 50 (YouTube API limit)
for i in range(0, len(all_video_ids), 50):
    if len(all_video_data) >= TARGET_VIDEOS:
        break  # We already have enough valid videos
    
    batch_ids = all_video_ids[i:i+50]
    video_ids_str = ",".join(batch_ids)
    
    url = "https://www.googleapis.com/youtube/v3/videos"
    params = {"part": "snippet,contentDetails,statistics,status", "id": video_ids_str, "key": API_KEY}
    response = requests.get(url, params=params).json()
    
    # Check if videos are available
    if "items" in response:
        for item in response["items"]:
            # Only add if we haven't reached our target
            if len(all_video_data) < TARGET_VIDEOS:
                snippet = item["snippet"]
                stats = item.get("statistics", {})
                
                # Handle tags
                tags = snippet.get("tags", [])
                tags_str = ",".join(tags) if tags else ""
                
                # Create video data
                video_row = {
                    "id": item["id"],
                    "title": snippet.get("title", ""),
                    "description": snippet.get("description", ""),
                    "publishedAt": snippet.get("publishedAt", ""),
                    "tags": tags_str,
                    "categoryId": snippet.get("categoryId", ""),
                    "defaultLanguage": snippet.get("defaultLanguage", ""),
                    "defaultAudioLanguage": snippet.get("defaultAudioLanguage", ""),
                    "thumbnail_default": snippet["thumbnails"]["default"]["url"],
                    "thumbnail_high": snippet["thumbnails"]["high"]["url"],
                    "duration": item["contentDetails"].get("duration", ""),
                    "viewCount": stats.get("viewCount", "0"),
                    "likeCount": stats.get("likeCount", "0"),
                    "commentCount": stats.get("commentCount", "0"),
                    "privacyStatus": item.get("status", {}).get("privacyStatus", ""),
                }
                video_row.update(channel_info)
                all_video_data.append(video_row)
                processed_ids.append(item["id"])
    
    # Show progress
    print(f"  Batch {i//50 + 1}: Found {len(response.get('items', []))} available videos")

#  Save to CSV
if all_video_data:
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=all_video_data[0].keys())
        writer.writeheader()
        writer.writerows(all_video_data)
    
    print(f"\nSUCCESS: {len(all_video_data)} valid videos saved to {OUTPUT_FILE}")
    print(f" Details:")
    print(f"   - Requested: {TARGET_VIDEOS} videos")
    print(f"   - Retrieved: {len(all_video_data)} valid videos")
    print(f"   - Checked: {len(all_video_ids)} total IDs from playlist")
    
    # Show sample
    if all_video_data:
        print(f"\n Sample video:")
        print(f"   Title: {all_video_data[0]['title'][:60]}...")
        print(f"   ID: {all_video_data[0]['id']}")
        print(f"   Views: {all_video_data[0]['viewCount']}")
else:
    print("❌ No valid videos found to save!")