# Contributing to dagster-magento

Thank you for contributing to **dagster-magento**, a reusable Dagster library
for Magento 2's REST API: the `MagentoResource` primitives, the catalog import
layer built on top of them, and the native csv/json/xlsx file adapters.

## Getting Started

1. **Fork and clone** `github.com/ddtcorex/dagster-magento`.
2. Create a virtual environment and install the dev extras (Python 3.10+):

   ```bash
   python3 -m venv .venv
   .venv/bin/pip install -e ".[dev]"
   ```

3. Run the suite:

   ```bash
   .venv/bin/pytest -q
   ```

Source lives in `dagster_magento/`, tests in `tests/` (one module per source
module). `sandbox/`, `tests/.samples/` and `.venv/` are gitignored.

## Superpowers 3-Phase Workflow

Cross-repo work, or anything that changes a contract another repository
depends on, follows the workspace Superpowers workflow in order:
**brainstorming** (explore intent, requirements and design), **writing-plans**
(a task-by-task plan with test and implementation sketches) and
**executing-plans** (strict TDD: failing test first, verify RED, implement,
verify GREEN, commit that task, then the next). Specs and plans live at the
workspace root, never in this repository. Do not skip ahead to implementation
and do not commit while tests are red.

## Branch Naming

Never commit directly to `master`. Start a branch per work session:

- `fix/<topic>` for bug fixes
- `feat/<topic>` for new features
- `docs/<topic>` for documentation-only changes
- `chore/<topic>` for tooling, CI and repository setup

Rebase (not merge) when the base moves:
`git fetch origin && git rebase origin/master`.

## Conventional Commits

All commit subjects follow [Conventional Commits](https://www.conventionalcommits.org/)
in imperative mood:

```
<type>(<scope>): <subject>

<body: why, not what>

Refs: #<issue>
```

- **Types (closed list):** `feat` `fix` `docs` `chore` `refactor` `perf` `test` `build` `ci` `revert`
- **Scope:** optional, the module without the package prefix, for example
  `fix(formats):`, `feat(resource):`
- **Subject:** imperative, lowercase first word, at most 72 characters, no
  trailing period
- **Body:** explain why and the trade-offs when the change is non-trivial
- **Breaking changes:** `feat!: <subject>` plus a `BREAKING CHANGE:` footer

One TDD task is one commit while executing a plan.

## Validation

Run these before opening a PR:

```bash
.venv/bin/pytest -q                  # hermetic: requests_mock, no network
.venv/bin/pytest -m samples -q       # public sample files, needs network
```

The `live` marker needs a real Magento and is not expected from every
contributor; the maintainers run it against the govard sandbox:

```bash
scripts/sandbox.sh up && scripts/sandbox.sh consumers
eval "$(scripts/sandbox.sh env)"
MAGENTO_CA_BUNDLE=~/.govard/ssl/root.crt .venv/bin/pytest -m live -q
```

Do not claim verified, done or clean without having run the checks: be ready
to paste the exact command output in the PR. A live test may be red for a
documented environment defect (see `tests/live/test_catalog_e2e.py`); never
weaken its assertion to make it green.

## Pull Requests

1. Push your branch and open a PR into `master`.
2. Fill out `.github/PULL_REQUEST_TEMPLATE.md` (Summary, Why, Changes,
   Validation, Linked Issues).
3. Link the PR to the plan that produced it when the Superpowers workflow was
   used.
4. CI must be green before merge, and the maintainer approves the merge.

## Privacy Boundary

This repository is public. Keep private project and client names, local
absolute paths, tokens and credentials out of code, docs, tests and commit
messages: git history ships with the repository and cannot be unpublished.
Run the workspace blacklist gate before pushing.

## Code of Conduct

This project follows the [Contributor Covenant Code of Conduct](./CODE_OF_CONDUCT.md).
By participating, you agree to its terms.

## Questions or Security Reports

- General questions: open a GitHub Discussion or issue.
- Security vulnerabilities: use GitHub's private advisory reporting at
  `https://github.com/ddtcorex/dagster-magento/security/advisories`, not a
  public issue.

## License

By contributing, you agree that your contributions are licensed under the
[MIT License](./LICENSE).
