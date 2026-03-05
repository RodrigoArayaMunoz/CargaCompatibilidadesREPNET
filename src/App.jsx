import Logo from "/logo.png";
import "./App.css";
import { useState } from "react";

function App() {
  const [file, setFile] = useState(null);
  const [status, setStatus] = useState("idle"); // idle, processing, success, error
  const [message, setMessage] = useState("");

  const API_BASE = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";

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
    const selectedFile = e.target.files?.[0];
    if (!selectedFile) return;

    // Aceptamos Excel (.xlsx) y opcionalmente CSV
    if (!isExcelFile(selectedFile) && !isCsvFile(selectedFile)) {
      setFile(null);
      setStatus("error");
      setMessage("Archivo no válido. Selecciona un Excel (.xlsx) o CSV (.csv).");
      return;
    }

    setFile(selectedFile);
    setStatus("idle");
    setMessage("");
  };

  const uploadFile = async (fileToUpload) => {
    const formData = new FormData();
    formData.append("file", fileToUpload);

    const isExcel = fileToUpload.name.toLowerCase().endsWith(".xlsx");
    const endpoint = isExcel ? "/imports-excel" : "/imports";

    const res = await fetch(`${API_BASE}${endpoint}`, {
      method: "POST",
      body: formData,
    });

    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      const detail = data?.detail || data?.message || "Error subiendo el archivo.";
      throw new Error(typeof detail === "string" ? detail : "Error subiendo el archivo.");
    }
    if (!data?.job_id) throw new Error("No se recibió job_id del servidor.");
    return data.job_id;
  };

  const startJob = async (jobId) => {
    const res = await fetch(`${API_BASE}/imports/${jobId}/start`, { method: "POST" });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      const detail = data?.detail || data?.message || "No se pudo iniciar el procesamiento.";
      throw new Error(typeof detail === "string" ? detail : "No se pudo iniciar el procesamiento.");
    }
    return data;
  };

  const pollJob = async (jobId) => {
    const poll = async () => {
      try {
        const r = await fetch(`${API_BASE}/imports/${jobId}`);
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
          setMessage("Archivo procesado exitosamente.");
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
    if (!file) {
      setStatus("error");
      setMessage("Debes seleccionar un archivo antes de iniciar el proceso.");
      return;
    }

    try {
      setStatus("processing");

      const isExcel = file.name.toLowerCase().endsWith(".xlsx");
      setMessage(isExcel ? "Subiendo Excel y convirtiendo..." : "Subiendo CSV...");

      // 1) Upload file -> /imports-excel o /imports
      const jobId = await uploadFile(file);

      // 2) Start job
      setMessage("Iniciando procesamiento...");
      await startJob(jobId);

      // 3) Poll status
      pollJob(jobId);
    } catch (error) {
      setStatus("error");
      setMessage(error?.message || "Ocurrió un error al procesar el archivo.");
    }
  };

  const GenerateDict = async () => {

  }

  const acceptText = "Archivo permitido: .xlsx o .csv";
  const buttonText = status === "processing" ? "Procesando..." : "Procesar Archivo";
  const buttonDict = status === "processing" ? "Procesando..." : "Generar Diccionario de SKU-PUBLICACIONES";

  return (
    <div className="container">
      <h1 className="title">Carga de Compatibilidades</h1>
      <img src={Logo} alt="Logo" className="logo" />

            <button
        className="process-button"
        onClick={GenerateDict}
        disabled={status === "processing"}
      >
        {buttonDict}
      </button>

      <div className="file-wrapper">
        <label className="file-label" htmlFor="fileInput">
          📂 Elegir archivo (Excel o CSV)
        </label>

        <input
          id="fileInput"
          className="file-input"
          type="file"
          accept=".xlsx,.csv,text/csv,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
          onChange={handleFileChange}
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
        disabled={status === "processing"}
      >
        {buttonText}
      </button>

      {message && <p className={`status-message ${status}`}>{message}</p>}
    </div>
  );
}

export default App;