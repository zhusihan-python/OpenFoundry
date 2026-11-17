import json
import os
import subprocess
import sys
import tempfile
import unittest
from verification_plan import plan, ALL

VALIDATION_PATHS = [
    'apps/web/scripts/lint.mjs',
    'apps/web/scripts/test-quality-rules.mjs',
    'apps/web/package.json',
    'apps/web/playwright.config.ts',
    'apps/web/playwright.ofexcel.config.ts',
    'apps/web/playwright.excel.config.ts',
    'apps/web/playwright.p31-real.config.ts',
    'apps/web/vite.config.ts',
    'apps/web/vite.p31-e2e.config.ts',
    'apps/web/vitest.config.ts',
    'apps/web/vitest.unit.config.mts',
    'apps/web/playwright.config.cjs',
]
PRODUCT_PATHS = [
    'apps/web/src/lib/components/Trail.tsx',
    'apps/web/e2e/workshop-filter-list.spec.ts',
    'apps/web/src/lib/utils/vitest.config.ts',
]

SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'verification_plan.py')

class PlanTests(unittest.TestCase):
    def test_backend_cannot_omit_real_excel(self):
        self.assertIn('real-excel', plan(['services/dataset-versioning-service/internal/repo/publish.go']))
    def test_shared_change_expands(self):
        self.assertTrue(ALL <= set(plan(['libs/core-models/dataset.go'])))
    def test_unknown_fails_conservatively(self):
        self.assertTrue(ALL <= set(plan(['new-system/config.json'])))
    def test_docs_do_not_hide_code(self):
        self.assertIn('frontend', plan(['docs/index.md', 'apps/web/src/routes/datasets/New.tsx']))
    def test_policy_requires_human(self):
        self.assertIn('human-review', plan(['apps/web/scripts/lint-baseline.json']))
    def test_frontend_validation_entrypoints_require_review(self):
        for path in VALIDATION_PATHS:
            with self.subTest(path=path):
                self.assertIn('frontend', plan([path]))
                self.assertIn('human-review', plan([path]))
    def test_ordinary_frontend_files_do_not_require_review(self):
        for path in PRODUCT_PATHS:
            with self.subTest(path=path):
                self.assertEqual(plan([path]), ['frontend'])
    def test_no_changes_is_empty(self):
        self.assertEqual(plan([]), [])
    def test_policy_docs_need_human_review(self):
        self.assertIn('human-review', plan(['docs/agent-automation/verification-policy.md']))
        self.assertIn('docs-links', plan(['docs/agent-automation/verification-policy.md']))
    def test_contributing_needs_human_review(self):
        self.assertIn('human-review', plan(['CONTRIBUTING.md']))

class CliTests(unittest.TestCase):
    """Run the CLI against a real temporary git repository."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.repo = os.path.join(self._tmp.name, 'repo')
        os.makedirs(self.repo)
        self.git('init', '-q', '-b', 'main')
        self.git('config', 'user.email', 'plan-tests@example.invalid')
        self.git('config', 'user.name', 'plan-tests')
        for rel, content in {
            'AGENTS.md': '# agents\n',
            'README.md': '# repo\n',
            'apps/web/vite.p31-e2e.config.ts': 'export default {}\n',
            'services/dataset-versioning-service/internal/repo/publish.go': 'package repo\n',
        }.items():
            self.write(rel, content)
        self.git('add', '-A')
        self.git('commit', '-qm', 'base')
        self.base = self.git('rev-parse', 'HEAD')

    def tearDown(self):
        self._tmp.cleanup()

    def git(self, *args):
        return subprocess.check_output(['git', '-C', self.repo, *args], text=True).strip()

    def write(self, rel, content):
        path = os.path.join(self.repo, rel)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w', encoding='utf-8') as fh:
            fh.write(content)

    def run_plan(self, base):
        return subprocess.run([sys.executable, SCRIPT, base], cwd=self.repo,
                              capture_output=True, text=True)

    def report(self, base):
        result = self.run_plan(base)
        self.assertEqual(result.returncode, 0, result.stderr)
        return json.loads(result.stdout)

    def test_clean_tree_at_base_is_empty_not_run(self):
        report = self.report(self.base)
        self.assertEqual(report['base_sha'], self.base)
        self.assertEqual(report['paths'], [])
        self.assertEqual(report['required_categories'], [])
        self.assertEqual(report['execution_status'], 'not-run')

    def test_committed_change_since_base_is_included(self):
        self.write('services/dataset-versioning-service/internal/repo/publish.go',
                   'package repo // changed\n')
        self.git('commit', '-qam', 'touch service')
        report = self.report(self.base)
        self.assertIn('services/dataset-versioning-service/internal/repo/publish.go', report['paths'])
        self.assertIn('go-unit', report['required_categories'])
        self.assertIn('real-excel', report['required_categories'])

    def test_untracked_file_is_included(self):
        self.write('apps/web/src/lib/api/datasets.ts', 'export {}\n')
        report = self.report(self.base)
        self.assertIn('apps/web/src/lib/api/datasets.ts', report['paths'])
        self.assertIn('frontend', report['required_categories'])
        self.assertIn('real-excel', report['required_categories'])

    def test_deleted_path_still_selects_checks(self):
        self.git('rm', '-q', 'services/dataset-versioning-service/internal/repo/publish.go')
        report = self.report(self.base)
        self.assertIn('services/dataset-versioning-service/internal/repo/publish.go', report['paths'])
        self.assertIn('go-integration', report['required_categories'])
        self.assertIn('real-excel', report['required_categories'])

    def test_rename_counts_old_and_new_paths(self):
        self.git('mv', 'services/dataset-versioning-service/internal/repo/publish.go',
                 'services/dataset-versioning-service/internal/repo/store.go')
        report = self.report(self.base)
        self.assertIn('services/dataset-versioning-service/internal/repo/publish.go', report['paths'])
        self.assertIn('services/dataset-versioning-service/internal/repo/store.go', report['paths'])
        self.assertIn('real-excel', report['required_categories'])

    def test_sensitive_rule_change_requires_human_review(self):
        self.write('AGENTS.md', '# agents\n# tightened acceptance rules\n')
        report = self.report(self.base)
        self.assertIn('AGENTS.md', report['paths'])
        self.assertIn('human-review', report['required_categories'])

    def test_frontend_path_matrix_through_real_cli(self):
        for path in VALIDATION_PATHS + PRODUCT_PATHS:
            with self.subTest(path=path):
                tracked = path == 'apps/web/vite.p31-e2e.config.ts'
                self.write(path, '// changed validation or product file\n')
                try:
                    report = self.report(self.base)
                    self.assertIn(path, report['paths'])
                    self.assertEqual(report['execution_status'], 'not-run')
                    self.assertIn('frontend', report['required_categories'])
                    if path in VALIDATION_PATHS:
                        self.assertIn('human-review', report['required_categories'])
                    else:
                        self.assertEqual(report['required_categories'], ['frontend'])
                finally:
                    if tracked:
                        self.write(path, 'export default {}\n')
                    else:
                        os.remove(os.path.join(self.repo, path))

    def test_unknown_path_selects_all_not_run(self):
        self.write('new-system/config.json', '{}\n')
        report = self.report(self.base)
        self.assertEqual(set(report['required_categories']), ALL)
        self.assertEqual(report['execution_status'], 'not-run')

    def test_deleted_validation_config_requires_review(self):
        path = 'apps/web/vite.p31-e2e.config.ts'
        self.git('rm', '-q', path)
        report = self.report(self.base)
        self.assertIn(path, report['paths'])
        self.assertIn('human-review', report['required_categories'])
        self.assertEqual(report['execution_status'], 'not-run')

    def test_validation_config_renamed_to_product_path_keeps_review(self):
        old = 'apps/web/vite.p31-e2e.config.ts'
        new = 'apps/web/product.ts'
        self.git('mv', old, new)
        report = self.report(self.base)
        self.assertIn(old, report['paths'])
        self.assertIn(new, report['paths'])
        self.assertIn('human-review', report['required_categories'])
        self.assertEqual(report['execution_status'], 'not-run')

    def test_invalid_base_fails_without_plan(self):
        result = self.run_plan('no-such-ref')
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn('execution_status', result.stdout)
        self.assertTrue(result.stderr.strip())

if __name__ == '__main__': unittest.main()
