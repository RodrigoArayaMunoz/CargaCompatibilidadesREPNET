import { Menu } from "lucide-react";

export default function Topbar({ title, onMenuClick }) {
  return (
    <header className="topbar">
      <div className="topbar__inner">
        <div className="topbar__left">
          <button
            className="topbar__menu-button"
            onClick={onMenuClick}
            aria-label="Abrir menú"
          >
            <Menu size={19} />
          </button>

          <div className="topbar__text">
            <h2 className="topbar__title">{title}</h2>
          </div>
        </div>
      </div>
    </header>
  );
}