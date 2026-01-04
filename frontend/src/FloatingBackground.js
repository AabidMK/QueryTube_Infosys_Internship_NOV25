import React, { useEffect, useRef } from "react";

function FloatingBackground() {
  const canvasRef = useRef(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    const ctx = canvas.getContext("2d");
    canvas.width = window.innerWidth;
    canvas.height = window.innerHeight;

    const iconSize = 24;
    const spacing = 80;
    const speed = 0.7;

    // Single rail positions
    const leftX = canvas.width * 0.065;   // center-left
    const rightX = canvas.width * 0.935;  // center-right

    const numIcons = Math.ceil(canvas.height / spacing) + 2;

    const leftIcons = Array.from({ length: numIcons }, (_, i) => ({
      text: "🎥",
      x: leftX,
      y: i * spacing
    }));

    const rightIcons = Array.from({ length: numIcons }, (_, i) => ({
      text: "📝",
      x: rightX,
      y: canvas.height - i * spacing
    }));

    function animate() {
      ctx.clearRect(0, 0, canvas.width, canvas.height);

      // Draw single neon rails
      ctx.strokeStyle = "#00ffff";
      ctx.lineWidth = 2;

      ctx.beginPath();
      ctx.moveTo(leftX, 0);
      ctx.lineTo(leftX, canvas.height);
      ctx.stroke();

      ctx.beginPath();
      ctx.moveTo(rightX, 0);
      ctx.lineTo(rightX, canvas.height);
      ctx.stroke();

      // Left icons flow down
      leftIcons.forEach(icon => {
        ctx.font = `${iconSize}px "Segoe UI Emoji", sans-serif`;
        ctx.fillStyle = "#00ffff";
        ctx.fillText(icon.text, icon.x - iconSize / 2+9.8, icon.y);

        icon.y += speed;
        if (icon.y > canvas.height + spacing) {
          const minY = Math.min(...leftIcons.map(i => i.y));
          icon.y = minY - spacing;
        }
      });

      // Right icons flow up
      rightIcons.forEach(icon => {
        ctx.font = `${iconSize}px "Segoe UI Emoji", sans-serif`;
        ctx.fillStyle = "#00ffff";
        ctx.fillText(icon.text, icon.x - iconSize / 2+6.9, icon.y);

        icon.y -= speed;
        if (icon.y < -spacing) {
          const maxY = Math.max(...rightIcons.map(i => i.y));
          icon.y = maxY + spacing;
        }
      });

      requestAnimationFrame(animate);
    }

    animate();

    window.addEventListener("resize", () => {
      canvas.width = window.innerWidth;
      canvas.height = window.innerHeight;
    });
  }, []);

  return (
    <canvas
      ref={canvasRef}
      style={{ position: "fixed", top: 0, left: 0, zIndex: -1 }}
    />
  );
}

export default FloatingBackground;
