import "./ResultModal.css";

function ResultModal({ open, onClose, summary, results }) {
  if (!open) return null;

  const safeSummary = summary || {};
  const safeResults = Array.isArray(results) ? results : [];

  return (
    <div className="result-modal-overlay" onClick={onClose}>
      <div
        className="result-modal"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="result-modal-header">
          <h2 className="result-modal-title">Resultado del procesamiento</h2>
          <button className="result-modal-close" onClick={onClose}>
            ✕
          </button>
        </div>

        <div className="result-modal-summary-grid">
          <div className="result-modal-summary-card">
            <span className="result-modal-summary-label">Total procesadas</span>
            <strong className="result-modal-summary-value">
              {safeSummary.processed_rows ?? 0}
            </strong>
          </div>

          <div className="result-modal-summary-card result-modal-summary-card-success">
            <span className="result-modal-summary-label">Compatibilidades OK</span>
            <strong className="result-modal-summary-value">
              {safeSummary.success_count ?? 0}
            </strong>
          </div>

          <div className="result-modal-summary-card result-modal-summary-card-error">
            <span className="result-modal-summary-label">Errores</span>
            <strong className="result-modal-summary-value">
              {safeSummary.error_count ?? 0}
            </strong>
          </div>
        </div>

        <div className="result-modal-body">
          <h3 className="result-modal-section-title">Detalle</h3>

          {safeResults.length === 0 ? (
            <p className="result-modal-empty">No hay resultados para mostrar.</p>
          ) : (
            <div className="result-modal-list">
              {safeResults.map((item, index) => {
                const itemId = item?.item_id || "Sin ITEM_ID";
                const itemResults = Array.isArray(item?.results)
                  ? item.results
                  : [];
                const mainReason = item?.reason || "";
                const itemOk = item?.ok === true;

                return (
                  <div
                    key={`${itemId}-${index}`}
                    className={`result-modal-card ${
                      itemOk
                        ? "result-modal-card-success"
                        : "result-modal-card-error"
                    }`}
                  >
                    <div className="result-modal-card-header">
                      <div>
                        <strong className="result-modal-item-title">
                          {itemId}
                        </strong>
                        <div className="result-modal-item-status">
                          {itemOk ? "✅ Procesado" : "❌ Con errores"}
                        </div>
                      </div>
                    </div>

                    {mainReason ? (
                      <div className="result-modal-reason-box">{mainReason}</div>
                    ) : null}

                    {itemResults.length > 0 && (
                      <div className="result-modal-sub-list">
                        {itemResults.map((sub, subIndex) => (
                          <div
                            key={`${itemId}-sub-${subIndex}`}
                            className={`result-modal-sub-card ${
                              sub?.ok
                                ? "result-modal-sub-card-success"
                                : "result-modal-sub-card-error"
                            }`}
                          >
                            <div className="result-modal-sub-row">
                              <span>
                                <strong>Año:</strong> {sub?.year ?? "-"}
                              </span>
                              <span>{sub?.ok ? "✅ OK" : "❌ Error"}</span>
                            </div>

                            {sub?.product_id ? (
                              <div className="result-modal-meta-line">
                                <strong>Product ID:</strong> {sub.product_id}
                              </div>
                            ) : null}

                            {sub?.reason ? (
                              <div className="result-modal-meta-line">
                                <strong>Motivo:</strong> {sub.reason}
                              </div>
                            ) : null}
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

export default ResultModal;