import { BrowserRouter, Route, Routes } from "react-router-dom";
import MainLayout from "../src/frontend/layouts/MainLayout";
import Home from "../src/frontend/pages/Home";
import Compatibilities from "./frontend/pages/CompatibilitiesUpload";
import PreciosStock from "./frontend/pages/PriceStocksUploads";
import NoCompatibilidades from "./frontend/pages/NoCompatibilities";
import MercadoLibreConnected from "./frontend/pages/MercadoLibreConnected";
import { MercadoLibreConnectionProvider } from "./frontend/context/MercadoLibreConnectionContext";

export default function App() {
  return (
    <MercadoLibreConnectionProvider>
      <BrowserRouter>
        <Routes>
          <Route path="/ml-connected" element={<MercadoLibreConnected />} />

          <Route element={<MainLayout />}>
            <Route path="/" element={<Home />} />
            <Route
              path="/compatibilidades/carga-masiva"
              element={<Compatibilities />}
            />
            <Route
              path="/compatibilidades/no-compatibilidades"
              element={<NoCompatibilidades />}
            />
            <Route
              path="/actualizaciones/precios-stock"
              element={<PreciosStock />}
            />
          </Route>
        </Routes>
      </BrowserRouter>
    </MercadoLibreConnectionProvider>
  );
}