import { useEffect, useMemo, useState } from "react";
import "./PublicationsWithoutCompatibilityModal.css";

function PublicationsWithoutCompatibilityModal({ open, onClose, apiBase }) {
  const [items, setItems] = useState([]);
  const [filteredText, setFilteredText] = useState("");
  const [filterType, setFilterType] = useState("mlc");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => {
    if (!open) return;

    const fetchPublications = async () => {
      try {
        setLoading(true);
        setError("");

        const res = await fetch(
          `${apiBase}/publications/without-compatibilities`,
          {
            method: "GET",
            credentials: "include",
          }
        );

        const data = await res.json().catch(() => []);

        if (!res.ok) {
          throw new Error(
            data?.detail ||
              data?.message ||
              "No se pudieron cargar las publicaciones sin compatibilidades."
          );
        }

        setItems(Array.isArray(data) ? data : []);
      } catch (err) {
        setError(
          err?.message ||
            "Ocurrió un error al obtener las publicaciones sin compatibilidades."
        );
      } finally {
        setLoading(false);
      }
    };

    fetchPublications();
  }, [open, apiBase]);

  const filteredItems = useMemo(() => {
    const text = filteredText.trim().toLowerCase();

    if (!text) return items;

    return items.filter((item) => {
      const mlc = String(item?.mlc || "").toLowerCase();
      const title = String(item?.title || "").toLowerCase();

      if (filterType === "mlc") {
        return mlc.includes(text);
      }

      return title.includes(text);
    });
  }, [items, filteredText, filterType]);

  if (!open) return null;

  return (
    <div className="custom-modal-overlay" onClick={onClose}>
      <div
        className="custom-modal-container publications-modal"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="custom-modal-header">
          <h2>Publicaciones sin compatibilidades</h2>
          <button className="custom-modal-close" onClick={onClose}>
            ×
          </button>
        </div>

        <div className="publications-toolbar">
          <select
            className="publications-filter-type"
            value={filterType}
            onChange={(e) => setFilterType(e.target.value)}
          >
            <option value="mlc">Buscar por MLC</option>
            <option value="title">Buscar por título</option>
          </select>

          <input
            type="text"
            className="publications-search-input"
            placeholder={
              filterType === "mlc"
                ? "Escribe un MLC..."
                : "Escribe un título..."
            }
            value={filteredText}
            onChange={(e) => setFilteredText(e.target.value)}
          />
        </div>

        <div className="publications-body">
          {loading ? (
            <div className="publications-state">Cargando publicaciones...</div>
          ) : error ? (
            <div className="publications-state error">{error}</div>
          ) : (
            <>
              <div className="publications-count">
                Total encontrados: {filteredItems.length}
              </div>

              <div className="publications-table-wrapper">
                <table className="publications-table">
                  <thead>
                    <tr>
                      <th>MLC</th>
                      <th>Título</th>
                    </tr>
                  </thead>
                  <tbody>
                    {filteredItems.length === 0 ? (
                      <tr>
                        <td colSpan="2" className="empty-row">
                          No se encontraron publicaciones.
                        </td>
                      </tr>
                    ) : (
                      filteredItems.map((item, index) => (
                        <tr key={`${item?.mlc || "item"}-${index}`}>
                          <td>{item?.mlc || "-"}</td>
                          <td>{item?.title || "-"}</td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  );
}

export default PublicationsWithoutCompatibilityModal;
