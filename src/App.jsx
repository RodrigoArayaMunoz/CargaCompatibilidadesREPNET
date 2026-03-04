
import Logo from '/logo.png'
import './App.css'
import { useState } from 'react';
function App() {
  
  
  const [file,setFile] = useState(null);
  const [status, setStatus] = useState("idle"); //Estados esperados: idle, processing, success, error
  const [message, setMessage] = useState("");

  const handleFileChange = (e) => {
    const selectedFile = e.target.files[0];

    if (!selectedFile) return 
      if (selectedFile.type !=="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" && selectedFile.type !== "application/vnd.ms-excel") {
        setStatus("error");
        setMessage("Archivo no válido. Por favor, selecciona un archivo Excel (.xlsx o .xls).");
        
        return;
      } 

    setFile(selectedFile);
    setStatus("idle");
    setMessage("");
    
  }
  const handleProcess = async () => {
    if (!file){
      setStatus("error");
      setMessage("Debes seleccionar un archivo antes de iniciar el proceso.");
      return;
    }

    try{
      setStatus("processing");
      setMessage("Procesando el archivo, por favor espera...");

      // Simulación de procesoo de carga
      await new Promise((resolve) => setTimeout(resolve, 3000)); // Simula un proceso de 3 segundos

      setStatus("success");
      setMessage("Archivo procesado exitosamente.");
    } catch (error){
      setStatus("error");
      setMessage("Ocurrió un error al procesar el archivo. Por favor, intenta nuevamente.");
    }
  }

  

  return(
      <div className='container'>
        <h1 className='title'>Carga de Compatibilidades</h1>
        <img src={Logo} alt="Logo Vite" className='logo' />

<div className="file-wrapper">
  <label className="file-label" htmlFor="fileInput">
    📂 Elegir archivo Excel
  </label>
  <input
    id="fileInput"
    className="file-input"
    type="file"
    accept=".xlsx,.xls"
    onChange={handleFileChange}
  />
  {/* Muestra el nombre del archivo seleccionado */}
  <span className="file-name">
    {file ? file.name : "Ningún archivo seleccionado"}
  </span>
</div>

        <button className='process-button' onClick={handleProcess}
        disabled ={status === "processing"}>
          {status === "processing" ? "Procesando..." : "Procesar Excel"}
          </button>

          {message && (
            <p className={`status-message ${status}`}>
              {message}
            </p>
          )}
      </div>
  );
}

export default App
