"""Failure controls for joint CI conclusions and commit ancestry."""
import contextlib
from pathlib import Path
import subprocess
import tempfile
import unittest

import joint_verification as joint


class JointVerificationTests(unittest.TestCase):
    def success_needs(self):
        return {name: {'result': 'success'} for name in joint.REQUIRED_JOBS}

    def test_success_does_not_claim_human_approval(self):
        result = joint.aggregate(self.success_needs())
        self.assertEqual(result['automatic_gates'], 'success')
        self.assertEqual(result['human_approval'], 'not-claimed')

    def test_every_non_success_result_fails_closed(self):
        for name in joint.REQUIRED_JOBS:
            for result in ['failure', 'cancelled', 'skipped', '', None, 'waiting']:
                with self.subTest(job=name, result=result):
                    needs = self.success_needs()
                    needs[name]['result'] = result
                    with self.assertRaises(ValueError):
                        joint.aggregate(needs)

    def test_missing_extra_and_malformed_needs_fail_closed(self):
        for needs in [{}, [], None, self.success_needs() | {'undeclared': {'result': 'success'}}]:
            with self.subTest(needs=needs), self.assertRaises(ValueError):
                joint.aggregate(needs)
        for name in joint.REQUIRED_JOBS:
            needs = self.success_needs()
            del needs[name]
            with self.assertRaises(ValueError):
                joint.aggregate(needs)
            needs = self.success_needs()
            needs[name] = 'success'
            with self.assertRaises(ValueError):
                joint.aggregate(needs)

    def test_ancestry_and_identity_against_real_git_history(self):
        with tempfile.TemporaryDirectory() as tmp, contextlib.chdir(tmp):
            subprocess.run(['git', 'init', '-q'], check=True)
            def commit(value):
                Path('value').write_text(value)
                subprocess.run(['git', 'add', 'value'], check=True)
                subprocess.run(['git', '-c', 'user.name=Joint CI fixture', '-c',
                                'user.email=fixture@example.invalid', 'commit', '-qm', value], check=True)
                return joint.git('rev-parse', 'HEAD')
            base = commit('base')
            head = commit('head')
            joint.validate_base(base, head)
            for invalid in ['', '0' * 40, head, 'HEAD', 'not-a-revision']:
                with self.subTest(base=invalid), self.assertRaises(ValueError):
                    joint.validate_base(invalid, head)
            subprocess.run(['git', 'checkout', '--orphan', 'other', '-q'], check=True)
            other = commit('unrelated')
            with self.assertRaises(subprocess.CalledProcessError):
                joint.validate_base(base, other)
            with self.assertRaises(ValueError):
                joint.preflight(head, joint.PILOT_BASE)
            with self.assertRaises(ValueError):
                joint.preflight(other, base)


if __name__ == '__main__':
    unittest.main()
