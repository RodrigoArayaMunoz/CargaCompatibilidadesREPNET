import "../styles/MercadoLibreConnectionModal.css";
import { useMercadoLibreConnection } from "../context/MercadoLibreConnectionContext";

export default function MercadoLibreConnectionModal() {
  const { loading, connected, message, connectMercadoLibre } =
    useMercadoLibreConnection();

  if (loading || connected) return null;

  return (
    <div className="ml-global-modal-overlay">
      <div className="ml-global-modal">
        <h2 className="ml-global-modal__title">
          Conexión global requerida
        </h2>

        <p className="ml-global-modal__text">
          Para usar la plataforma, primero debes conectar la cuenta global de
          Mercado Libre.
        </p>

        <p className="ml-global-modal__status">
          {message || "No hay cuenta conectada"}
        </p>

        <button
          type="button"
          className="ml-global-modal__button"
          onClick={connectMercadoLibre}
        >
          Conectar cuenta de Mercado Libre
        </button>
      </div>
    </div>
  );
}