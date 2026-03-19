import { createContext, useContext, useEffect, useState, useCallback } from "react";

const MercadoLibreConnectionContext = createContext(null);

export function MercadoLibreConnectionProvider({ children }) {
  const [loading, setLoading] = useState(true);
  const [connected, setConnected] = useState(false);
  const [account, setAccount] = useState(null);
  const [message, setMessage] = useState("");

  const API_BASE =
    import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";

  const checkConnection = useCallback(async () => {
    try {
      setLoading(true);

      const res = await fetch(`${API_BASE}/ml/status`, {
        method: "GET",
        credentials: "include",
      });

      const data = await res.json().catch(() => ({}));

      if (res.ok && data?.connected) {
        setConnected(true);
        setAccount(data.account || null);
        setMessage(data.message || "");
      } else {
        setConnected(false);
        setAccount(null);
        setMessage(data?.message || "No hay cuenta conectada");
      }
    } catch (error) {
      setConnected(false);
      setAccount(null);
      setMessage("No se pudo verificar la conexión global con Mercado Libre");
    } finally {
      setLoading(false);
    }
  }, [API_BASE]);

  useEffect(() => {
    checkConnection();
  }, [checkConnection]);

  const connectMercadoLibre = () => {
    window.location.href = `${API_BASE}/auth/login`;
  };

  return (
    <MercadoLibreConnectionContext.Provider
      value={{
        loading,
        connected,
        account,
        message,
        checkConnection,
        connectMercadoLibre,
      }}
    >
      {children}
    </MercadoLibreConnectionContext.Provider>
  );
}

export function useMercadoLibreConnection() {
  const context = useContext(MercadoLibreConnectionContext);
  if (!context) {
    throw new Error(
      "useMercadoLibreConnection debe usarse dentro de MercadoLibreConnectionProvider"
    );
  }
  return context;
}