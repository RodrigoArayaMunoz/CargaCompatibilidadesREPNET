import logo from "../../../public/logo.png";
import "../styles/Home.css";

export default function Home() {
  return (
    <section className="home-page">
      <div className="content-panel content-panel--centered">
        <img src={logo} alt="Repnet" className="home-page__logo" />
      </div>
    </section>
  );
}