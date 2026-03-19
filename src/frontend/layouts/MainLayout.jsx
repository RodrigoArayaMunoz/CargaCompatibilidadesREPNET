import { useState } from "react";
import { Outlet } from "react-router-dom";
import Sidebar from "../components/SideBar";
import "../styles/MainLayout.css";

export default function MainLayout() {
  const [sidebarOpen, setSidebarOpen] = useState(false);

  return (
    <div className="layout">
      <Sidebar
        sidebarOpen={sidebarOpen}
        setSidebarOpen={setSidebarOpen}
      />

      <div className="layout__main">
        <main className="layout__content">
          <Outlet />
        </main>
      </div>
    </div>
  );
}