from __future__ import annotations

import os
import re
import stat
from pathlib import Path
from typing import Protocol

KRAB_TOKEN_RE = re.compile(r"^krab_(usr_[0-9a-f]{32})_([A-Za-z0-9_-]{43})$")


class CredentialError(RuntimeError):
    """A credential reference is invalid or cannot be resolved safely."""


class CredentialResolver(Protocol):
    def resolve(self, reference: str) -> str: ...


class PrivateFileCredentialResolver:
    """Resolve `file:` references without following links or weakening modes."""

    scheme = "file:"

    def reference(self, path: Path) -> str:
        resolved = path.expanduser().absolute()
        self._validate_parent(resolved.parent)
        self._read(resolved)
        return self.scheme + str(resolved)

    def resolve(self, reference: str) -> str:
        path = self._path_from_reference(reference)
        self._validate_parent(path.parent)
        return self._read(path)

    def resolve_path(self, reference: str) -> Path:
        """Return a validated credential path for a read-only sandbox mount."""

        path = self._path_from_reference(reference)
        self._validate_parent(path.parent)
        self._read(path)
        return path.resolve(strict=True)

    def _path_from_reference(self, reference: str) -> Path:
        if not isinstance(reference, str) or not reference.startswith(self.scheme):
            raise CredentialError("unsupported credential reference")
        raw_path = reference[len(self.scheme) :]
        if not raw_path or not Path(raw_path).is_absolute():
            raise CredentialError("file credential reference must use an absolute path")
        return Path(raw_path)

    @staticmethod
    def _validate_parent(path: Path) -> None:
        try:
            info = path.lstat()
        except OSError as exc:
            raise CredentialError(
                f"credential directory is unavailable: {path}"
            ) from exc
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise CredentialError("credential parent must be a private directory")
        if hasattr(os, "getuid") and info.st_uid != os.getuid():
            raise CredentialError("credential parent is not owned by the current user")
        if info.st_mode & 0o077:
            raise CredentialError("credential parent directory must use mode 0700")

    @staticmethod
    def _read(path: Path) -> str:
        flags = os.O_RDONLY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(path, flags)
        except FileNotFoundError as exc:
            raise CredentialError(f"credential file not found: {path}") from exc
        except OSError as exc:
            raise CredentialError("credential file must not be a symlink") from exc
        try:
            info = os.fstat(descriptor)
            if not stat.S_ISREG(info.st_mode):
                raise CredentialError("credential file must be a regular file")
            if hasattr(os, "getuid") and info.st_uid != os.getuid():
                raise CredentialError(
                    "credential file is not owned by the current user"
                )
            if info.st_nlink != 1:
                raise CredentialError("credential file must not be hard-linked")
            if info.st_mode & 0o077:
                raise CredentialError(
                    "credential file must not be accessible by group or other users"
                )
            raw = os.read(descriptor, 512)
            if os.read(descriptor, 1):
                raise CredentialError("credential file is unexpectedly large")
        finally:
            os.close(descriptor)
        try:
            token = raw.decode("ascii").strip()
        except UnicodeDecodeError as exc:
            raise CredentialError("credential file is not ASCII") from exc
        if KRAB_TOKEN_RE.fullmatch(token) is None:
            raise CredentialError("credential file does not contain a Krab token")
        return token
