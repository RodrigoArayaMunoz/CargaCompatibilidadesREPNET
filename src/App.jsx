import { BrowserRouter, Route, Routes } from "react-router-dom";
import MainLayout from "../src/frontend/layouts/MainLayout";
import Home from "../src/frontend/pages/Home";
import Compatibilidades from "./frontend/pages/CompatibilitiesUpload";
import PreciosStock from "./frontend/pages/PriceStocksUploads";

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route element={<MainLayout />}>
          <Route path="/" element={<Home />} />
          <Route path="/compatibilidades" element={<Compatibilidades />} />
          <Route path="/precios-stock" element={<PreciosStock />} />
        </Route>
      </Routes>
    </BrowserRouter>
  );
}