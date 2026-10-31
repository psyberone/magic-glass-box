#!/usr/bin/env python3
"""Run Mutmut with PyJWT's crypto module graph pinned in the parent process."""

from __future__ import annotations


def _pin_pyjwt_crypto() -> None:
    # Mutmut snapshots sys.modules, runs its covered-line pass, then unloads
    # everything imported by that pass before collecting test associations.
    # Re-importing cryptography's extension-backed class graph in the same
    # interpreter leaves PyJWT with incompatible HashAlgorithm identities.
    # Importing PyJWT first makes that graph part of Mutmut's baseline snapshot.
    import jwt

    _ = (jwt.PyJWK, jwt.PyJWT)


def main() -> None:
    _pin_pyjwt_crypto()
    from mutmut.__main__ import cli

    cli()


if __name__ == "__main__":
    main()
