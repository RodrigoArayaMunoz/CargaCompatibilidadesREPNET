import { useEffect } from "react";
import { useNavigate } from "react-router-dom";
import { useMercadoLibreConnection } from "../context/MercadoLibreConnectionContext";

export default function MercadoLibreConnected() {
  const navigate = useNavigate();
  const { checkConnection } = useMercadoLibreConnection();

  useEffect(() => {
    const run = async () => {
      await checkConnection();
      navigate("/", { replace: true });
    };

    run();
  }, [checkConnection, navigate]);

  return null;
}