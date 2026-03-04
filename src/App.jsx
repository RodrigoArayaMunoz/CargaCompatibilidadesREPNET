import Logo from "/logo.png";
import "./App.css";
import { useState } from "react";

function App() {
  const [file, setFile] = useState(null);
  const [status, setStatus] = useState("idle"); // idle, processing, success, error
  const [message, setMessage] = useState("");

  const isCsvFile = (f) => {
    if (!f) return false;

    const nameOk = f.name?.toLowerCase().endsWith(".csv");

    // En muchos casos CSV viene como text/csv, pero a veces viene vacío o como application/vnd.ms-excel
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

    if (!isCsvFile(selectedFile)) {
      setFile(null);
      setStatus("error");
      setMessage("Archivo no válido. Por favor, selecciona un archivo CSV (.csv).");
      return;
    }

    setFile(selectedFile);
    setStatus("idle");
    setMessage("");
  };

  const handleProcess = async () => {
    if (!file) {
      setStatus("error");
      setMessage("Debes seleccionar un archivo CSV antes de iniciar el proceso.");
      return;
    }

    try {
      setStatus("processing");
      setMessage("Procesando el archivo CSV, por favor espera...");

      // Simulación de proceso (aquí luego reemplazas por fetch a tu FastAPI)
      await new Promise((resolve) => setTimeout(resolve, 2000));

      setStatus("success");
      setMessage("CSV procesado exitosamente.");
    } catch (error) {
      setStatus("error");
      setMessage("Ocurrió un error al procesar el CSV. Por favor, intenta nuevamente.");
    }
  };

  return (
    <div className="container">
      <h1 className="title">Carga de Compatibilidades</h1>
      <img src={Logo} alt="Logo" className="logo" />

      <div className="file-wrapper">
        <label className="file-label" htmlFor="fileInput">
          📂 Elegir archivo CSV
        </label>

        <input
          id="fileInput"
          className="file-input"
          type="file"
          accept=".csv,text/csv"
          onChange={handleFileChange}
        />

        <span className="file-name">
          {file ? file.name : "Ningún archivo seleccionado"}
        </span>
      </div>

      <button
        className="process-button"
        onClick={handleProcess}
        disabled={status === "processing"}
      >
        {status === "processing" ? "Procesando..." : "Procesar CSV"}
      </button>

      {message && <p className={`status-message ${status}`}>{message}</p>}
    </div>
  );
}

export default App;