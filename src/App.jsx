
import Logo from '/logo.png'
import './App.css'

function App() {
  const handleProcess = () => {
    console.log("Iniciando proceso de carga de compatibilidades...")
    // En esta sección se debe conectar al backend API para obtener los datos de compatibilidad y luego procesarlos
  }

  return(
      <div className='container'>
        <h1 className='title'>Carga de Compatibilidades</h1>
        <img src={Logo} alt="Logo Vite" className='logo' />
        <button className='process-button' onClick={handleProcess}>Iniciar proceso</button>
      </div>
  );
}

export default App
