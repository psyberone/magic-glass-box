from __future__ import annotations

import unittest
from pathlib import PurePosixPath

from magic_glass_box.path_safety import UnsafeSymlinkError, validate_symlink_graph


class SymlinkGraphSafetyTests(unittest.TestCase):
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
