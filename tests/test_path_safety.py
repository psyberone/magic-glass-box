from __future__ import annotations

import ast
import inspect
import textwrap
import unittest
from pathlib import PurePosixPath
from unittest import mock

from magic_glass_box import (
    credentials,
    execution_provider_cli,
    github_provider_cli,
    jira_provider_cli,
    openai_chatgpt,
    openai_chatgpt_cli,
    remote_runner,
)
from magic_glass_box.path_safety import (
    MAX_PATH_BYTES,
    MAX_PATH_DEPTH,
    UnsafePathError,
    UnsafeSymlinkError,
    read_bounded_descriptor,
    validate_repo_path,
    validate_symlink_graph,
    validate_symlink_target,
)


class BoundedDescriptorReadTests(unittest.TestCase):
    def test_retries_interruptions_and_joins_irregular_short_reads(self) -> None:
        with mock.patch(
            "magic_glass_box.path_safety.os.read",
            side_effect=(InterruptedError(), b"a", b"bc", b""),
        ) as read:
            self.assertEqual(read_bounded_descriptor(17, maximum=3), b"abc")

        self.assertEqual(read.call_count, 4)

    def test_reads_one_byte_past_the_limit_without_waiting_for_eof(self) -> None:
        with mock.patch(
            "magic_glass_box.path_safety.os.read",
            side_effect=(b"ab", b"cd"),
        ) as read:
            self.assertEqual(read_bounded_descriptor(17, maximum=3), b"abcd")

        self.assertEqual(read.call_args_list, [mock.call(17, 4), mock.call(17, 2)])

    def test_zero_descriptor_and_zero_limit_are_valid(self) -> None:
        with mock.patch(
            "magic_glass_box.path_safety.os.read", return_value=b""
        ) as read:
            self.assertEqual(read_bounded_descriptor(0, maximum=0), b"")

        read.assert_called_once_with(0, 1)

    def test_rejects_invalid_descriptor_and_limit_values(self) -> None:
        cases = (
            (-1, 1),
            (True, 1),
            (1, -1),
            (1, True),
            ("1", 1),
            (1, "1"),
        )

        for descriptor, maximum in cases:
            with (
                self.subTest(descriptor=descriptor, maximum=maximum),
                self.assertRaisesRegex(ValueError, "non-negative integers"),
            ):
                read_bounded_descriptor(  # type: ignore[arg-type]
                    descriptor, maximum=maximum
                )

    def test_all_private_loaders_use_the_shared_bounded_reader(self) -> None:
        loaders = (
            credentials.PrivateFileCredentialResolver._read_bytes,
            execution_provider_cli._read_file,
            remote_runner._private_file,
            openai_chatgpt_cli.load_config,
            openai_chatgpt.ChatGPTProfileStore._read,
            jira_provider_cli._read_private_file,
            jira_provider_cli.load_spec,
            github_provider_cli.load_spec,
        )

        for loader in loaders:
            with self.subTest(loader=loader.__qualname__):
                tree = ast.parse(textwrap.dedent(inspect.getsource(loader)))
                direct_reads = [
                    node
                    for node in ast.walk(tree)
                    if isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "read"
                    and isinstance(node.func.value, ast.Name)
                    and node.func.value.id == "os"
                ]
                self.assertEqual(direct_reads, [])


class RepositoryPathSafetyTests(unittest.TestCase):
    def test_accepts_the_shared_byte_and_depth_boundaries(self) -> None:
        byte_boundary = "a" * MAX_PATH_BYTES
        depth_boundary = "/".join("a" for _ in range(MAX_PATH_DEPTH))

        self.assertEqual(validate_repo_path(byte_boundary), byte_boundary)
        self.assertEqual(validate_repo_path(depth_boundary), depth_boundary)

    def test_rejects_cross_platform_git_aliases_and_unsafe_shapes(self) -> None:
        cases = (
            "",
            ".",
            "../escape",
            "/absolute",
            "back\\slash",
            "trailing/",
            "a//b",
            ".git/config",
            "dir/.GiT/config",
            "dir/.git./config",
            "dir/.git:stream/config",
            "dir/git~1/config",
            "dir/.g\u200dit/config",
            "dir/.\u200cgit./config",
            "dir/.\u200cgit:stream/config",
            "dir/git\u200c~1/config",
            "a" * (MAX_PATH_BYTES + 1),
            "/".join("a" for _ in range(MAX_PATH_DEPTH + 1)),
        )
        for value in cases:
            with self.subTest(value=value), self.assertRaises(UnsafePathError):
                validate_repo_path(value)


class SymlinkGraphSafetyTests(unittest.TestCase):
    def test_rejects_git_aliases_and_backslashes_in_targets(self) -> None:
        for target in (
            ".git/config",
            ".GiT/hooks/post-checkout",
            ".git./config",
            ".git:stream/config",
            "git~1/config",
            ".g\u200dit/config",
            "directory\\file",
        ):
            with (
                self.subTest(target=target),
                self.assertRaises(UnsafeSymlinkError),
            ):
                validate_symlink_target("link", target)

    def test_allows_nested_links_that_resolve_inside_the_root(self) -> None:
        validate_symlink_graph(
            {
                PurePosixPath("links/inner"): "../shared",
                PurePosixPath("links/outer"): "inner/file.txt",
            }
        )

    def test_rejects_direct_and_recursive_escapes(self) -> None:
        cases = (
            {PurePosixPath("escape"): "../outside"},
            {
                PurePosixPath("evil/x"): "..",
                PurePosixPath("evil/y"): "x/..",
            },
        )
        for links in cases:
            with (
                self.subTest(links=links),
                self.assertRaisesRegex(
                    UnsafeSymlinkError,
                    "^symlink graph escapes its root$",
                ),
            ):
                validate_symlink_graph(links)

    def test_rejects_escapes_composed_through_portable_path_aliases(self) -> None:
        cases = (
            {
                PurePosixPath("d/up"): "..",
                PurePosixPath("escape"): "D/up/..",
            },
            {
                PurePosixPath("caf\u00e9/up"): "..",
                PurePosixPath("escape"): "cafe\u0301/up/..",
            },
        )
        for links in cases:
            with (
                self.subTest(links=links),
                self.assertRaisesRegex(
                    UnsafeSymlinkError,
                    "^symlink graph escapes its root$",
                ),
            ):
                validate_symlink_graph(links)

    def test_rejects_aliased_symlink_sources(self) -> None:
        with self.assertRaisesRegex(
            UnsafeSymlinkError,
            "^symlink graph contains portable path aliases$",
        ):
            validate_symlink_graph(
                {
                    PurePosixPath("LINK"): "one",
                    PurePosixPath("link"): "two",
                }
            )

    def test_rejects_invalid_absolute_and_cyclic_targets(self) -> None:
        cases: tuple[tuple[dict[PurePosixPath, object], str], ...] = (
            ({PurePosixPath("."): "target"}, "symlink path is invalid"),
            ({PurePosixPath("../bad-path"): "target"}, "symlink path is invalid"),
            ({PurePosixPath("/absolute-path"): "target"}, "symlink path is invalid"),
            ({PurePosixPath("empty"): ""}, "symlink target is invalid"),
            ({PurePosixPath("absolute"): "/outside"}, "symlink target is absolute"),
            ({PurePosixPath("nul"): "bad\x00target"}, "symlink target is invalid"),
            ({PurePosixPath("wrong-type"): 7}, "symlink target is invalid"),
        )
        for links, message in cases:
            with (
                self.subTest(links=links),
                self.assertRaisesRegex(
                    UnsafeSymlinkError,
                    f"^{message}$",
                ),
            ):
                validate_symlink_graph(links)  # type: ignore[arg-type]

    def test_rejects_cycles_and_growth_at_the_complexity_bound(self) -> None:
        with self.assertRaisesRegex(
            UnsafeSymlinkError,
            "^symlink graph is too complex$",
        ):
            validate_symlink_graph({PurePosixPath("a"): "b", PurePosixPath("b"): "a"})
        with self.assertRaisesRegex(
            UnsafeSymlinkError,
            "^symlink graph is too complex$",
        ):
            validate_symlink_graph({PurePosixPath("a"): "a/x"})

    def test_expands_links_supplied_by_the_destination_lookup(self) -> None:
        def lookup(path: PurePosixPath) -> str | None:
            return ".." if path == PurePosixPath("evil/x") else None

        with self.assertRaisesRegex(
            UnsafeSymlinkError,
            "^symlink graph escapes its root$",
        ):
            validate_symlink_graph(
                {PurePosixPath("evil/y"): "x/.."},
                lookup=lookup,
            )


if __name__ == "__main__":
    unittest.main()
