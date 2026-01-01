import React from 'react';
import { useNavigate } from 'react-router-dom';

const VideoCard = ({ video }) => {
  const navigate = useNavigate();

  return (
    <div
      style={{
        border: "1px solid #ddd",
        padding: 10,
        marginBottom: 10,
        cursor: "pointer"
      }}
      onClick={() => navigate(`/summary/${video.video_id}`)}
    >
      <img src={video.thumbnail} alt="" width="200" />
      <h3>{video.title}</h3>
      <p>{video.channel}</p>
      <p>
        Views: {video.views} | Likes: {video.likes} | Duration: {video.duration}
      </p>
      <small>Score: {video.similarity_score?.toFixed(3)}</small>
    </div>
  );
};

export default VideoCard;
