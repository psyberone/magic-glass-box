from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from magic_glass_box.credentials import (
    CredentialError,
    PrivateFileCredentialResolver,
)

USER_ID = "usr_" + "1" * 32
TOKEN = f"krab_{USER_ID}_" + "A" * 43


class PrivateFileCredentialResolverTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        os.chmod(self.root, 0o700)
        self.path = self.root / "token"
        self.path.write_text(TOKEN + "\n", encoding="ascii")
        os.chmod(self.path, 0o600)
        self.resolver = PrivateFileCredentialResolver()

    def test_reference_contains_path_but_resolve_returns_token(self) -> None:
        reference = self.resolver.reference(self.path)
        self.assertEqual(reference, "file:" + str(self.path))
        self.assertEqual(self.resolver.resolve(reference), TOKEN)
        self.assertEqual(self.resolver.resolve_path(reference), self.path.resolve())
        self.assertNotIn(TOKEN, reference)

    def test_rejects_public_file(self) -> None:
        os.chmod(self.path, 0o644)
        with self.assertRaisesRegex(CredentialError, "group or other"):
            self.resolver.reference(self.path)

    def test_rejects_symlink(self) -> None:
        link = self.root / "linked-token"
        link.symlink_to(self.path)
        with self.assertRaises(CredentialError):
            self.resolver.reference(link)

    def test_rejects_public_parent(self) -> None:
        os.chmod(self.root, 0o755)  # noqa: S103 - deliberately insecure fixture
        with self.assertRaisesRegex(CredentialError, "0700"):
            self.resolver.reference(self.path)

    def test_rejects_unsupported_and_relative_references(self) -> None:
        with self.assertRaises(CredentialError) as unsupported:
            self.resolver.resolve("env:KRAB_TOKEN")
        self.assertEqual(str(unsupported.exception), "unsupported credential reference")
        with self.assertRaises(CredentialError) as relative:
            self.resolver.resolve("file:relative-token")
        self.assertEqual(
            str(relative.exception),
            "file credential reference must use an absolute path",
        )

    def test_rejects_missing_and_invalid_token_files(self) -> None:
        with self.assertRaisesRegex(CredentialError, "not found"):
            self.resolver.resolve("file:" + str(self.root / "missing"))
        self.path.write_text("not-a-token\n", encoding="ascii")
        with self.assertRaisesRegex(CredentialError, "does not contain"):
            self.resolver.reference(self.path)

    def test_rejects_non_ascii_and_oversized_files(self) -> None:
        self.path.write_bytes("å".encode() + b"\n")
        with self.assertRaisesRegex(CredentialError, "not ASCII"):
            self.resolver.reference(self.path)
        self.path.write_bytes(b"A" * 513)
        with self.assertRaisesRegex(CredentialError, "unexpectedly large"):
            self.resolver.reference(self.path)

    def test_rejects_non_directory_parent(self) -> None:
        parent = self.root / "not-a-directory"
        parent.write_text("x", encoding="ascii")
        with self.assertRaisesRegex(CredentialError, "private directory"):
            self.resolver.resolve("file:" + str(parent / "token"))

    def test_rejects_hard_linked_and_non_regular_files(self) -> None:
        linked = self.root / "hard-link"
        os.link(self.path, linked)
        with self.assertRaisesRegex(CredentialError, "hard-linked"):
            self.resolver.reference(self.path)

        directory = self.root / "credential-directory"
        directory.mkdir(mode=0o600)
        with self.assertRaisesRegex(CredentialError, "regular file"):
            self.resolver.reference(directory)

    def test_rejects_unowned_parent_and_file(self) -> None:
        actual_uid = os.getuid()
        with mock.patch(
            "magic_glass_box.credentials.os.getuid", return_value=actual_uid + 1
        ):
            with self.assertRaisesRegex(CredentialError, "parent is not owned"):
                self.resolver.reference(self.path)
            with self.assertRaisesRegex(CredentialError, "file is not owned"):
                self.resolver._read(self.path)

    def test_error_messages_are_stable_without_embedding_credentials(self) -> None:
        os.chmod(self.root, 0o755)  # noqa: S103 - deliberately insecure fixture
        with self.assertRaises(CredentialError) as raised:
            self.resolver.reference(self.path)
        self.assertEqual(
            str(raised.exception),
            "credential parent directory must use mode 0700",
        )
        self.assertNotIn(TOKEN, str(raised.exception))


if __name__ == "__main__":
    unittest.main()
