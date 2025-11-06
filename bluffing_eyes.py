"""Legacy wrapper for backwards compatibility.

The tabletop experiment is now launched via :mod:`run_game`.
"""

from __future__ import annotations

import warnings

warnings.warn(
    "bluffing_eyes.py ist veraltet. Bitte verwenden Sie run_game.py als Startpunkt.",
    DeprecationWarning,
    stacklevel=2,
)


def main(*_: object, **__: object) -> None:
    """Raise a helpful error if legacy entry points are invoked."""

    raise RuntimeError(
        "Der Spielstart wurde nach run_game.py verlegt. "
        "Bitte führen Sie `python run_game.py` aus."
    )
