import Logo from "/logo.png";
import "./App.css";
import { useEffect, useState, useRef } from "react";
import ResultModal from "./components/ResultModal";
import PublicationsWithoutCompatibilityModal from "./components/PublicationsWithoutCompatibilityModal";


function ProcessingOverlay({ visible, progress = 0, message = "" }) {
  if (!visible) return null;

  return (
    <div className="processing-overlay">
      <div className="processing-box">
        <div className="processing-spinner" />
        <h2>Procesando compatibilidades</h2>
        <p className="processing-progress">{progress}%</p>
        <p className="processing-message">
          {message || "Procesando archivo..."}
        </p>

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

function App() {
  const fileInputRef = useRef(null);
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
  const [loadingResult, setLoadingResult] = useState(false);

  const [jobId, setJobId] = useState(null);
  const [loadingProcess, setLoadingProcess] = useState(false);
  const [progress, setProgress] = useState(0);
  const [processMessage, setProcessMessage] = useState("");
  const [showResultModal, setShowResultModal] = useState(false);
  const [showPublicationsModal, setShowPublicationsModal] = useState(false);

  const API_BASE =
    import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";

  useEffect(() => {
    checkMlConnection();
  }, []);

  const handleCloseResultModal = () => {
  setShowResultModal(false);

  setFile(null);
  setJobId(null);
  setJobResult(null);

  setProgress(0);
  setProcessMessage("");
  setStatus("idle");
  setMessage("");

  if (fileInputRef.current) {
    fileInputRef.current.value = "";
  }
};

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
      setJobId(null);
      setStatus("error");
      setMessage("Archivo no válido. Selecciona un Excel (.xlsx) o CSV (.csv).");
      return;
    }

    setFile(selectedFile);
    setJobId(null);
    setStatus("idle");
    setMessage("");
    setJobResult(null);
    setShowResultModal(false);
    setProgress(0);
    setProcessMessage("");
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

  const startJob = async (currentJobId) => {
    const res = await fetch(`${API_BASE}/imports/${currentJobId}/start`, {
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

  const fetchJobResult = async (currentJobId) => {
    const res = await fetch(`${API_BASE}/imports/${currentJobId}/result`, {
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
  };

  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

  const pollJob = async (currentJobId) => {
    let finished = false;

    while (!finished) {
      try {
        const r = await fetch(`${API_BASE}/imports/${currentJobId}`, {
          credentials: "include",
        });

        const data = await r.json().catch(() => ({}));

        if (!r.ok) {
          setLoadingProcess(false);
          setLoadingResult(false);
          setStatus("error");
          setMessage("Error consultando el estado del proceso.");
          return;
        }

        const currentProgress =
          typeof data.progress === "number" ? data.progress : 0;

        setProgress(currentProgress);
        setProcessMessage(data.message || "");
        setMessage(data.message || "");

        if (data.status === "success") {
          finished = true;
          setProgress(100);
          //setProcessMessage("Archivo procesado exitosamente. Cargando resumen final...");
          //setMessage("Archivo procesado exitosamente. Cargando resumen final...");
          setStatus("success");

          try {
            setLoadingResult(true);
            await fetchJobResult(currentJobId);
          } catch (error) {
            setStatus("error");
            setMessage(
              error?.message ||
                "El proceso terminó, pero no se pudo obtener el resumen."
            );
          } finally {
            setLoadingResult(false);
            setLoadingProcess(false);
          }

          return;
        }

        if (data.status === "error") {
          finished = true;
          setLoadingProcess(false);
          setLoadingResult(false);
          setStatus("error");
          setMessage(data.message || "Ocurrió un error al procesar el archivo.");
          return;
        }

        await sleep(1200);
      } catch (err) {
        setLoadingProcess(false);
        setLoadingResult(false);
        setStatus("error");
        setMessage("Error de red consultando el estado del proceso.");
        return;
      }
    }
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
      setLoadingProcess(true);
      setLoadingResult(false);
      setProgress(0);
      setProcessMessage("Subiendo archivo...");
      setMessage("");

      const isExcel = file.name.toLowerCase().endsWith(".xlsx");
      setProcessMessage(isExcel ? "Subiendo Excel..." : "Subiendo CSV...");

      const newJobId = await uploadFile(file);
      setJobId(newJobId);

      setProgress(5);
      setProcessMessage("Iniciando procesamiento...");
      await startJob(newJobId);

      await pollJob(newJobId);
    } catch (error) {
      setLoadingProcess(false);
      setLoadingResult(false);
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

    const handleViewPublicationsWithoutCompatibilities = () => {
      setShowPublicationsModal(true);
    };

    const handleClosePublicationsModal = () => {
      setShowPublicationsModal(false);
    };

  return (
    <>
      <ProcessingOverlay
        visible={loadingProcess}
        progress={progress}
        message={processMessage}
      />

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
  ref={fileInputRef}
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

<div className="actions-row">
  <button
    className="process-button"
    onClick={handleProcess}
    disabled={
      !mlVerified ||
      !file ||
      status === "processing" ||
      checkingConnection ||
      loadingResult ||
      loadingProcess
    }
  >
    {loadingResult
      ? "Cargando resumen..."
      : loadingProcess
      ? "Procesando..."
      : buttonText}
  </button>

  <button
    className="process-button secondary-action-button"
    onClick={handleViewPublicationsWithoutCompatibilities}
    disabled={!mlVerified || checkingConnection || loadingProcess || loadingResult}
  >
    Ver Publicaciones sin compatibilidades
  </button>
</div>

        {message && !loadingProcess && (
          <p className={`status-message ${status}`}>{message}</p>
        )}
      </div>

<ResultModal
  open={showResultModal}
  onClose={handleCloseResultModal}
  summary={jobResult?.summary}
  results={jobResult?.results}
/>

<PublicationsWithoutCompatibilityModal
  open={showPublicationsModal}
  onClose={handleClosePublicationsModal}

/>
    </>
  );
}

export default App;