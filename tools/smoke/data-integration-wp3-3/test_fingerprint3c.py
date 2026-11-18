#!/usr/bin/env python3
"""Regression tests for smoke evidence collected from a committed checkout."""

import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


FINGERPRINT = Path(__file__).with_name("fingerprint3c.py").resolve()
HARNESS_DIR = Path("tools/smoke/data-integration-wp3-3")


class FingerprintTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.repo = Path(tmp.name) / "repo"
        self.evidence = Path(tmp.name) / "results"
        self.repo.mkdir()
        self.evidence.mkdir()
        self.source = self.repo / HARNESS_DIR / "fixture.py"
        self.source.parent.mkdir(parents=True)
        self.source.write_bytes(b"original")
        self.git("init", "-q")
        self.git("add", ".")
        self.git("-c", "user.name=Test", "-c", "user.email=test@example.invalid",
                 "-c", "commit.gpgSign=false", "commit", "-qm", "Fixture")

    def git(self, *args):
        return subprocess.check_output(["git", *args], cwd=self.repo)

    def fingerprint(self, mode):
        name = "fingerprint.json" if mode == "start" else "fingerprint_end.json"
        subprocess.run([sys.executable, "-I", str(FINGERPRINT), self.repo, self.evidence,
                        "--mode", mode], check=True, capture_output=True)
        return json.loads((self.evidence / name).read_text())

    def test_clean_checkout_hashes_tracked_harness_sources(self):
        self.assertEqual(self.git("status", "--porcelain"), b"")
        start = self.fingerprint("start")
        end = self.fingerprint("end")
        self.assertEqual(start["head"], self.git("rev-parse", "HEAD").decode().strip())
        self.assertEqual(start["entries"], [{
            "path": str(self.source.relative_to(self.repo)),
            "sha256": hashlib.sha256(b"original").hexdigest(),
            "status": "",
        }])
        self.assertEqual(start["working_tree_fingerprint"], end["working_tree_fingerprint"])

    def test_dirty_and_untracked_sources_change_fingerprint(self):
        start = self.fingerprint("start")
        self.source.write_bytes(b"modified")
        (self.source.parent / "added.py").write_bytes(b"new")
        end = self.fingerprint("end")
        self.assertNotEqual(start["working_tree_fingerprint"], end["working_tree_fingerprint"])
        self.assertEqual([entry["status"] for entry in end["entries"]], ["??", "M"])


if __name__ == "__main__":
    unittest.main()
