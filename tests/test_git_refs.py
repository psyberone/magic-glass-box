from __future__ import annotations

import unittest

from magic_glass_box.domain import ValidationError
from magic_glass_box.git_refs import (
    BRANCH_REF_PREFIX,
    MAX_BRANCH_REF_BYTES,
    normalize_branch_ref,
)


class BranchRefTests(unittest.TestCase):
    def test_short_ref_accepts_the_exact_byte_limit(self) -> None:
        branch = "x" * (MAX_BRANCH_REF_BYTES - len(BRANCH_REF_PREFIX))

        self.assertEqual(
            normalize_branch_ref(branch, field="branch", allow_short=True),
            f"{BRANCH_REF_PREFIX}{branch}",
        )

    def test_at_is_not_a_branch_name(self) -> None:
        with self.assertRaises(ValidationError):
            normalize_branch_ref(f"{BRANCH_REF_PREFIX}@", field="branch")

    def test_uppercase_x_remains_valid(self) -> None:
        branch_ref = f"{BRANCH_REF_PREFIX}feature/X-ray"

        self.assertEqual(
            normalize_branch_ref(branch_ref, field="branch"),
            branch_ref,
        )


if __name__ == "__main__":
    unittest.main()
