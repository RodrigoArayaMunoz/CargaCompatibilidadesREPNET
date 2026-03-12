import Logo from "/logo.png";
import "./App.css";
import { useEffect, useState } from "react";
import ResultModal from "./components/ResultModal";

function App() {
  const [file, setFile] = useState(null);
  const [status, setStatus] = useState("idle");
  const [message, setMessage] = useState("");

  const [mlConnected, setMlConnected] = useState(false);
  const [mlVerified, setMlVerified] = useState(false);
  const [checkingConnection, setCheckingConnection] = useState(true);
  const [mlStatusMessage, setMlStatusMessage] = useState(
    "Verificando conexión con Mercado Libre..."
  );

  const [jobResult, setJobResult] = useState(null);
  const [showResultModal, setShowResultModal] = useState(false);
  const [loadingResult, setLoadingResult] = useState(false);

  const API_BASE =
    import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";

  useEffect(() => {
    checkMlConnection();
  }, []);

  const checkMlConnection = async () => {
    try {
      setCheckingConnection(true);
      setMlVerified(false);
      setMlConnected(false);
      setMlStatusMessage("Verificando conexión con Mercado Libre...");

      const res = await fetch(`${API_BASE}/ml/status`, {
        method: "GET",
        credentials: "include",
      });

      const data = await res.json().catch(() => ({}));

      if (res.ok && data?.connected === true) {
        setMlConnected(true);
        setMlVerified(true);
        setMlStatusMessage("Conectado exitosamente");
      } else {
        setMlConnected(false);
        setMlVerified(false);
        setMlStatusMessage("Debes conectar tu cuenta de Mercado Libre");
      }
    } catch (error) {
      setMlConnected(false);
      setMlVerified(false);
      setMlStatusMessage("No se pudo verificar la conexión con Mercado Libre");
    } finally {
      setCheckingConnection(false);
    }
  };

  const isExcelFile = (f) => {
    if (!f) return false;
    const nameOk = f.name?.toLowerCase().endsWith(".xlsx");
    const typeOk =
      f.type ===
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" ||
      f.type === "" ||
      f.type === "application/octet-stream";
    return nameOk && typeOk;
  };

  const isCsvFile = (f) => {
    if (!f) return false;
    const nameOk = f.name?.toLowerCase().endsWith(".csv");
    const typeOk =
      f.type === "text/csv" ||
      f.type === "application/vnd.ms-excel" ||
      f.type === "" ||
      f.type === "application/csv";
    return nameOk && typeOk;
  };

  const handleFileChange = (e) => {
    if (!mlVerified) return;

    const selectedFile = e.target.files?.[0];
    if (!selectedFile) return;

    if (!isExcelFile(selectedFile) && !isCsvFile(selectedFile)) {
      setFile(null);
      setStatus("error");
      setMessage("Archivo no válido. Selecciona un Excel (.xlsx) o CSV (.csv).");
      return;
    }

    setFile(selectedFile);
    setStatus("idle");
    setMessage("");
    setJobResult(null);
  };

  const uploadFile = async (fileToUpload) => {
    const formData = new FormData();
    formData.append("file", fileToUpload);

    const isExcel = fileToUpload.name.toLowerCase().endsWith(".xlsx");
    const endpoint = isExcel ? "/imports-excel" : "/imports";

    const res = await fetch(`${API_BASE}${endpoint}`, {
      method: "POST",
      body: formData,
      credentials: "include",
    });

    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      const detail =
        data?.detail || data?.message || "Error subiendo el archivo.";
      throw new Error(
        typeof detail === "string" ? detail : "Error subiendo el archivo."
      );
    }

    if (!data?.job_id) {
      throw new Error("No se recibió job_id del servidor.");
    }

    return data.job_id;
  };

  const startJob = async (jobId) => {
    const res = await fetch(`${API_BASE}/imports/${jobId}/start`, {
      method: "POST",
      credentials: "include",
    });

    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      const detail =
        data?.detail ||
        data?.message ||
        "No se pudo iniciar el procesamiento.";
      throw new Error(
        typeof detail === "string"
          ? detail
          : "No se pudo iniciar el procesamiento."
      );
    }

    return data;
  };

  const fetchJobResult = async (jobId) => {
    setLoadingResult(true);

    try {
      const res = await fetch(`${API_BASE}/imports/${jobId}/result`, {
        method: "GET",
        credentials: "include",
      });

      const data = await res.json().catch(() => ({}));

      if (!res.ok) {
        throw new Error(
          data?.detail || data?.message || "No se pudo obtener el resultado final."
        );
      }

      setJobResult(data);
      setShowResultModal(true);
    } catch (error) {
      setMessage(
        error?.message || "El proceso terminó, pero no se pudo obtener el resumen."
      );
    } finally {
      setLoadingResult(false);
    }
  };

  const pollJob = async (jobId) => {
    const poll = async () => {
      try {
        const r = await fetch(`${API_BASE}/imports/${jobId}`, {
          credentials: "include",
        });

        const data = await r.json().catch(() => ({}));

        if (!r.ok) {
          setStatus("error");
          setMessage("Error consultando el estado del proceso.");
          return;
        }

        const progressText =
          typeof data.progress === "number" ? ` (${data.progress}%)` : "";
        setMessage(`${data.message || "Procesando..."}${progressText}`);

        if (data.status === "success") {
          setStatus("success");
          setMessage("Archivo procesado exitosamente. Cargando resumen final...");
          await fetchJobResult(jobId);
          return;
        }

        if (data.status === "error") {
          setStatus("error");
          setMessage(data.message || "Ocurrió un error al procesar el archivo.");
          return;
        }

        setTimeout(poll, 1500);
      } catch (err) {
        setStatus("error");
        setMessage("Error de red consultando el estado del proceso.");
      }
    };

    poll();
  };

  const handleProcess = async () => {
    if (!mlVerified) {
      setStatus("error");
      setMessage("Primero debes conectar tu cuenta de Mercado Libre.");
      return;
    }

    if (!file) {
      setStatus("error");
      setMessage("Debes seleccionar un archivo antes de iniciar el proceso.");
      return;
    }

    try {
      setShowResultModal(false);
      setJobResult(null);
      setStatus("processing");

      const isExcel = file.name.toLowerCase().endsWith(".xlsx");
      setMessage(isExcel ? "Subiendo Excel y convirtiendo..." : "Subiendo CSV...");

      const jobId = await uploadFile(file);

      setMessage("Iniciando procesamiento...");
      await startJob(jobId);

      pollJob(jobId);
    } catch (error) {
      setStatus("error");
      setMessage(error?.message || "Ocurrió un error al procesar el archivo.");
    }
  };

  const handleConnectMercadoLibre = () => {
    if (checkingConnection || mlVerified) return;
    window.location.href = `${API_BASE}/auth/login`;
  };

  const acceptText = "Archivo permitido: .xlsx o .csv";
  const buttonText =
    status === "processing" ? "Procesando..." : "Procesar Archivo";

  const connectButtonText = checkingConnection
    ? "Verificando conexión..."
    : mlVerified
    ? "✅ Cuenta conectada"
    : "Conectar con MercadoLibre";

  const statusText = checkingConnection
    ? "Verificando conexión con Mercado Libre..."
    : mlVerified
    ? ""
    : mlStatusMessage;

  return (
    <>
      <div className="container">
        <h1 className="title">Carga de Compatibilidades</h1>
        <img src={Logo} alt="Logo" className="logo" />

        <button
          className={`process-button-ml ${mlVerified ? "connected" : ""}`}
          onClick={handleConnectMercadoLibre}
          disabled={checkingConnection || mlVerified}
        >
          {connectButtonText}
        </button>

        <div className={`ml-status ${mlVerified ? "success" : "pending"}`}>
          {statusText}
        </div>

        <div className={`file-wrapper ${!mlVerified ? "disabled-section" : ""}`}>
          <label
            className={`file-label ${!mlVerified ? "disabled-label" : ""}`}
            htmlFor="fileInput"
          >
            📂 Elegir archivo (Excel o CSV)
          </label>

          <input
            id="fileInput"
            className="file-input"
            type="file"
            accept=".xlsx,.csv,text/csv,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            onChange={handleFileChange}
            disabled={!mlVerified || status === "processing" || checkingConnection}
          />

          <span className="file-name">
            {file ? file.name : "Ningún archivo seleccionado"}
          </span>

          <small style={{ display: "block", marginTop: 8, opacity: 0.7 }}>
            {acceptText}
          </small>
        </div>

        <button
          className="process-button"
          onClick={handleProcess}
          disabled={
            !mlVerified ||
            status === "processing" ||
            checkingConnection ||
            loadingResult
          }
        >
          {loadingResult ? "Cargando resumen..." : buttonText}
        </button>

        {message && <p className={`status-message ${status}`}>{message}</p>}
      </div>

      <ResultModal
        open={showResultModal}
        onClose={() => setShowResultModal(false)}
        summary={jobResult?.summary}
        results={jobResult?.results}
      />
    </>
  );
}

export default App;