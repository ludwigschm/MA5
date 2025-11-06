"""CLI entry point for launching the tabletop experiment."""

from __future__ import annotations

import argparse
import logging
import sys
from types import ModuleType
from typing import Optional, Sequence, Tuple

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


WINDOW_SIZE: Tuple[int, int] = (1280, 800)
WINDOW_MARGIN = 40


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Return parsed command line arguments for the game launcher."""

    parser = argparse.ArgumentParser(description="Starte das Bluffing-Eyes-Experiment")
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Startet ohne Eye-Tracking-Hardware (Demo-Modus).",
    )
    parser.add_argument(
        "--session",
        type=int,
        default=None,
        help="Optionale Session-ID für das Datenlogging.",
    )
    parser.add_argument(
        "--screen",
        type=int,
        choices=(1, 2),
        default=1,
        help="Wählt den Zielmonitor (1 = Hauptmonitor).",
    )
    return parser.parse_args(argv)


def _configure_window(screen_index: int) -> None:
    """Apply window size/position defaults before the Kivy app boots."""

    try:
        from kivy.core.window import Window
    except Exception as exc:  # pragma: no cover - optional dependency
        log.debug("Kivy-Window konnte nicht initialisiert werden: %s", exc)
        return

    Window.fullscreen = False
    Window.borderless = False
    Window.size = WINDOW_SIZE

    left = WINDOW_MARGIN
    top = WINDOW_MARGIN
    if screen_index == 2:
        try:
            import ctypes

            user32 = ctypes.windll.user32  # type: ignore[attr-defined]
            user32.SetProcessDPIAware()
            primary_width = user32.GetSystemMetrics(0)
        except Exception as exc:  # pragma: no cover - depends on OS
            log.warning(
                "Monitor 2 konnte nicht ausgewählt werden (falle auf Monitor 1 zurück): %s",
                exc,
            )
        else:
            left = int(primary_width) + WINDOW_MARGIN
    Window.left = left
    Window.top = top


def _load_tabletop_app() -> Optional[ModuleType]:
    """Safely import the tabletop application module."""

    try:
        import tabletop.app as tabletop_app
    except ModuleNotFoundError as exc:
        log.error(
            "Benötigtes Modul %s konnte nicht importiert werden. Bitte Abhängigkeiten installieren.",
            exc.name,
        )
        return None
    except SyntaxError as exc:
        location = f"{exc.filename}:{exc.lineno}" if exc.filename else "unbekannt"
        log.error(
            "Syntaxfehler beim Laden von tabletop.app (%s): %s",
            location,
            exc.msg,
        )
        return None
    except Exception as exc:
        log.exception("Unerwarteter Fehler beim Laden von tabletop.app: %s", exc)
        return None

    return tabletop_app


def _start_tabletop_app(
    tabletop_app: ModuleType, args: argparse.Namespace
) -> int:
    """Start the tabletop Kivy app and return an exit status code."""

    configure_logging = getattr(tabletop_app, "_configure_async_logging", None)
    logging_listener = None
    logging_queue = None
    if callable(configure_logging):
        logging_listener, logging_queue = configure_logging()

    bridge = None
    demo_mode = bool(args.demo)

    if not demo_mode:
        try:
            bridge = tabletop_app.PupilBridge()  # type: ignore[attr-defined]
            try:
                connected = bridge.connect()
            except Exception as exc:  # pragma: no cover - hardware dependent
                log.warning("Hardware nicht verbunden – Demo-Modus aktiviert")
                log.debug("Eye-Tracking-Verbindung fehlgeschlagen: %s", exc, exc_info=True)
                demo_mode = True
                bridge = None
            else:
                if not connected:
                    log.warning("Hardware nicht verbunden – Demo-Modus aktiviert")
                    demo_mode = True
                    bridge = None
        except ModuleNotFoundError as exc:
            log.error(
                "Eye-Tracking-Bibliothek %s fehlt – starte im Demo-Modus.",
                exc.name,
            )
            log.warning("Hardware nicht verbunden – Demo-Modus aktiviert")
            demo_mode = True
            bridge = None
        except Exception as exc:  # pragma: no cover - defensive fallback
            log.exception("Fehler beim Initialisieren der Eye-Tracking-Hardware: %s", exc)
            log.warning("Hardware nicht verbunden – Demo-Modus aktiviert")
            demo_mode = True
            bridge = None
    else:
        log.info("Demo-Modus aktiviert – Eye-Tracking-Hardware wird übersprungen.")

    desired_players = None
    if not demo_mode and bridge is not None:
        try:
            connected_players = set()
            try:
                connected_players = set(bridge.connected_players())
            except AttributeError:
                connected_players = set()
            resolver = getattr(tabletop_app, "_resolve_requested_players", None)
            if callable(resolver):
                desired_players = resolver("auto", connected=connected_players)
        except Exception as exc:  # pragma: no cover - defensive fallback
            log.debug("Spieler-Auflösung konnte nicht durchgeführt werden: %s", exc)
            desired_players = None

    app = tabletop_app.TabletopApp(
        session=args.session,
        block=None,
        player="auto",
        players=desired_players,
        bridge=bridge if not demo_mode else None,
        single_block_mode=False,
        logging_queue=logging_queue,
    )

    try:
        app.run()
    except Exception as exc:
        log.exception("Die Anwendung ist mit einem Fehler beendet: %s", exc)
        return 1
    finally:
        if not demo_mode and bridge is not None:
            try:
                desired = desired_players or []
                for tracked in desired:
                    try:
                        bridge.stop_recording(tracked)
                    except Exception:  # pragma: no cover - hardware dependent
                        log.exception(
                            "Stoppen der Aufnahme für %s fehlgeschlagen", tracked
                        )
                bridge.close()
            except Exception:  # pragma: no cover - defensive fallback
                log.exception("Fehler beim Aufräumen der Eye-Tracking-Hardware")
        if logging_listener is not None:
            logging_listener.stop()

    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """Main entry point used by ``python run_game.py``."""

    args = parse_args(argv)
    _configure_window(args.screen)
    tabletop_app = _load_tabletop_app()
    if tabletop_app is None:
        return 1

    return _start_tabletop_app(tabletop_app, args)


if __name__ == "__main__":
    sys.exit(main())
