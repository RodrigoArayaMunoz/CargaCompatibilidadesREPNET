import React from "react";
import "./ProcessingOverlay.css";

export default function ProcessingOverlay({ visible, progress = 0, message }) {
  if (!visible) return null;

  return (
    <div className="processing-overlay">
      <div className="processing-box">
        <div className="processing-spinner" />
        <h2>Procesando compatibilidades</h2>
        <p className="processing-progress">{progress}%</p>
        {message ? <p className="processing-message">{message}</p> : null}

        <div className="processing-bar">
          <div
            className="processing-bar-fill"
            style={{ width: `${progress}%` }}
          />
        </div>
      </div>
    </div>
  );
}