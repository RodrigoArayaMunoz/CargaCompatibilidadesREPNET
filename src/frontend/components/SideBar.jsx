import { NavLink } from "react-router-dom";
import { X, Boxes, BadgeDollarSign } from "lucide-react";
import logo from "../../../public/logo.png";
import "../styles/Sidebar.css";

const navItems = [
  {
    to: "/compatibilidades",
    label: "Carga de compatibilidades",
    icon: Boxes,
  },
  {
    to: "/precios-stock",
    label: "Actualización de precios y stock",
    icon: BadgeDollarSign,
  },
];

export default function Sidebar({ sidebarOpen, setSidebarOpen }) {
  return (
    <>
      <div
        className={`sidebar-overlay ${sidebarOpen ? "sidebar-overlay--show" : ""}`}
        onClick={() => setSidebarOpen(false)}
      />

      <aside className={`sidebar ${sidebarOpen ? "sidebar--open" : ""}`}>
        <div className="sidebar__header">
          <div className="sidebar__branding">
            <img src={logo} alt="Repnet" className="sidebar__brand-logo" />
          </div>

          <button
            className="sidebar__close"
            onClick={() => setSidebarOpen(false)}
            aria-label="Cerrar menú"
          >
            <X size={20} />
          </button>
        </div>

        <div className="sidebar__card">
          <p className="sidebar__card-label">Plataforma</p>
          <p className="sidebar__card-text">
            Gestiona compatibilidades y actualizaciones de precios y stock.
          </p>
        </div>

        <nav className="sidebar__nav">
          {navItems.map((item) => {
            const Icon = item.icon;

            return (
              <NavLink
                key={item.to}
                to={item.to}
                onClick={() => setSidebarOpen(false)}
                className={({ isActive }) =>
                  `sidebar__link ${isActive ? "sidebar__link--active" : ""}`
                }
              >
                <span className="sidebar__icon">
                  <Icon size={20} />
                </span>
                <span>{item.label}</span>
              </NavLink>
            );
          })}
        </nav>
      </aside>
    </>
  );
}