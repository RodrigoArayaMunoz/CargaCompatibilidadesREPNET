import "../styles/MercadoLibreConnectionLoader.css";
import { useMercadoLibreConnection } from "../context/MercadoLibreConnectionContext";

export default function MercadoLibreConnectionLoader() {
  const { loading } = useMercadoLibreConnection();

  if (!loading) return null;

  return (
    <div className="ml-global-loader-overlay">
      <div className="ml-global-loader-box">
        <div className="ml-global-loader-spinner" />
        <p className="ml-global-loader-text">
          Verificando conexión con Mercado Libre...
        </p>
      </div>
    </div>
  );
}