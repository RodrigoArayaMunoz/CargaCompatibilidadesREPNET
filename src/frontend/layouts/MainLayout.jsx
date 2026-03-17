import { useMemo, useState } from "react";
import { Outlet, useLocation } from "react-router-dom";
import Sidebar from "../components/SideBar";
import Topbar from "../components/TopBar";
import "../styles/MainLayout.css";

const TITLES = {
  "/": "Repnet",
  "/compatibilidades": "Carga de compatibilidades",
  "/precios-stock": "Actualización de precios y stock",
};

export default function MainLayout() {
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const location = useLocation();

  const title = useMemo(() => {
    return TITLES[location.pathname] || "Repnet";
  }, [location.pathname]);

  return (
    <div className="layout">
      <Sidebar
        sidebarOpen={sidebarOpen}
        setSidebarOpen={setSidebarOpen}
      />

      <div className="layout__main">
        <Topbar
          title={title}
          onMenuClick={() => setSidebarOpen(true)}
        />

        <main className="layout__content">
          <div className="layout__content-inner">
            <Outlet />
          </div>
        </main>
      </div>
    </div>
  );
}