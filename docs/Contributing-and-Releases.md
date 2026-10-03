# Contributing and Releases

dagster-magento is a public library distributed by git tag and URL install, never through PyPI. Contributions go through a branch and a pull request into `master`, with Conventional Commits, test first development, a hermetic test for every behaviour, and a privacy check because everything in the repository and its history is public. A release is a version bump, a changelog section, a fresh compatibility matrix and a tag; pushing the tag runs a workflow that creates the GitHub Release from the changelog. This page collects the rules from `CONTRIBUTING.md` and `AGENTS.md` and the exact release procedure.

## Setup

```bash
git clone https://github.com/<you>/dagster-magento.git
cd dagster-magento
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"
.venv/bin/pytest -q
```

Python 3.10 or newer. Source lives in `dagster_magento/`, tests in `tests/`. `sandbox/`, `tests/.samples/` and `.venv/` are gitignored. See [Sandbox-and-Testing](Sandbox-and-Testing) for the test layers.

## Workflow

### Branches

Never commit to `master` and never push it. One branch per work session:

| Prefix | Use |
| --- | --- |
| `feat/<topic>` | New features |
| `fix/<topic>` | Bug fixes |
| `docs/<topic>` | Documentation only |
| `chore/<topic>` | Tooling, CI, repository setup |

When the base moves, rebase, never merge `master` into the branch:

```bash
git fetch origin && git rebase origin/master
```

### Conventional Commits

```
<type>(<scope>): <subject>

<body: why, not what>

Refs: #<issue>
```

- Types (closed list): `feat` `fix` `docs` `chore` `refactor` `perf` `test` `build` `ci` `revert`.
- Scope: optional, the module without the package prefix, for example `fix(formats):`, `feat(resource):`.
- Subject: imperative, lowercase first word, at most 72 characters, no trailing period.
- Body: why and trade offs, for anything non trivial.
- Breaking change: `feat!: <subject>` plus a `BREAKING CHANGE:` footer.
- One logical change per commit; while executing a plan, one TDD task is one commit.

Work that crosses repositories, or changes a contract another repository depends on (the bridge module is the obvious one), follows a three phase workflow: brainstorming, writing a plan, then executing it task by task with strict TDD. Its specs and plans are kept outside this repository.

### Test first, hermetic first

- Write the failing test, confirm it fails for the expected reason, implement, confirm green. Never commit while tests are red.
- The default suite is hermetic: `requests_mock` only, no live HTTP, no real Magento. A test that needs a Magento to pass is a design mistake (`AGENTS.md`).
- Keep the shape: one test module per source module.
- When a behaviour depends on Magento's wire format, prefer a captured fixture over a hand written body (see Fixtures in [Sandbox-and-Testing](Sandbox-and-Testing)).
- When pinning Dagster behaviour, check the installed version's API instead of assuming; `AGENTS.md` notes it changed during this package's own development.
- A live test can be red for a documented environment defect; never weaken its assertion to make it green.

### Rules that are not negotiable

- `MagentoResource.password` stays `Field(repr=False, json_schema_extra={"dagster__is_secret": True})`, and no log line may interpolate the password, the token or raw request kwargs. The regression test `test_token_value_never_appears_in_logs_even_with_verbose_logging_and_401_retry` must be extended, not replaced.
- `MagentoAuthError` is not an `HTTPError` subclass and must never be swallowed by a catch log continue loop.
- `MagentoResource` stays a generic REST client; only the import layer may use the optional bridge module, through `BridgeClient`, and every path must also work without it (see [Optional-Bridge](Optional-Bridge)).
- No dependency on project specific companion endpoints.

### Validation before a pull request

```bash
.venv/bin/pytest -q                  # hermetic
.venv/bin/pytest -m samples -q       # needs network
```

The live layer is not expected from every contributor; maintainers run it against the sandbox. Do not claim verified, done or clean without the command output, and paste the exact commands and results into the PR.

### Pull requests

1. Push the branch and open a PR into `master`.
2. Fill in `.github/PULL_REQUEST_TEMPLATE.md`: Summary, Why, Changes (code, tests, `README.md` and `CHANGELOG.md` for user visible changes, `AGENTS.md` when a convention, boundary or command changed), Validation with pasted output, Linked Issues.
3. CI must be green: the `verify` job (Python 3.10) is the required check, and the 3.11 to 3.14 jobs must pass too.
4. A maintainer approves the merge. Merges and releases always need explicit human approval.

### Privacy boundary

The repository is public and its git history cannot be unpublished. Keep private project and client names, local absolute paths, tokens and credentials out of code, docs, tests, fixtures and commit messages. Run the workspace blacklist gate before pushing and keep its pre-commit hook installed. Recaptured fixtures are scrubbed of host and tokens, but still review them.

Security vulnerabilities go through GitHub's private advisory reporting for the repository, not a public issue.

## Release procedure

No registry: users install with

```bash
pip install "dagster-magento[xlsx] @ git+https://github.com/ddtcorex/dagster-magento.git@vX.Y.Z"
```

### 1. Changelog and version

- Move the `[Unreleased]` entries into a new section headed exactly `## [X.Y.Z] - YYYY-MM-DD` in `CHANGELOG.md` (Keep a Changelog format, Semantic Versioning). The release workflow looks for `[X.Y.Z]` in a `## [` heading, so the brackets matter.
- Set `version = "X.Y.Z"` in `pyproject.toml`.
- Update the version in the README install command.

### 2. Tests and the compatibility matrix

```bash
.venv/bin/pytest -q
.venv/bin/pytest -m samples -q
scripts/compat-matrix.sh --write
```

Commit the new `compat/results/` records and the regenerated results table in `docs/Compatibility.md`: a release never states a Magento line that has no current row. The matrix owns the sandbox for about an hour and a half; run no other live work and keep other govard sessions off the machine. Details in [Compatibility](Compatibility).

### 3. Merge, tag, push

After approval, with the release commit on `master` (through the normal PR):

```bash
git tag vX.Y.Z
git push origin vX.Y.Z
```

`master` reaches the remote through the merged PR; only the tag is pushed by hand (`git push origin vX.Y.Z`).

### 4. The release workflow

Pushing a tag matching `v*.*.*` runs `.github/workflows/release.yml` (permission `contents: write`):

1. Checks out the tag.
2. **Guard 1, version match:** strips the `v`, reads the first `version = "..."` line of `pyproject.toml`, and fails with `tag vX.Y.Z but pyproject.toml says ...` if they differ.
3. **Guard 2, changelog section:** extracts everything under the `## [X.Y.Z]` heading up to the next `## [` heading into `release-notes.md` and fails with `CHANGELOG.md has no section for X.Y.Z` if that is empty.
4. Creates the GitHub Release named after the tag with `softprops/action-gh-release@v2`, using the extracted notes as body plus GitHub's generated release notes.

Nothing is published to a registry; the workflow stops at the Release.

### Releasing a tag pushed before the workflow existed

The workflow also has a `workflow_dispatch` trigger with a required `tag` input. It runs the same steps against that existing tag:

```bash
gh workflow run release.yml -R ddtcorex/dagster-magento -f tag=vX.Y.Z
```

### 5. Verify the install from the tag

`AGENTS.md` requires verifying the README install command from a clean environment before calling a release done ("it shipped wrong once already"). For example:

```bash
python3 -m venv /tmp/dm-check
/tmp/dm-check/bin/pip install "dagster-magento[xlsx] @ git+https://github.com/ddtcorex/dagster-magento.git@vX.Y.Z"
/tmp/dm-check/bin/pip show dagster-magento          # Version: X.Y.Z
/tmp/dm-check/bin/python -c "import dagster_magento, openpyxl; print('ok')"
```

The package exposes no `__version__`, so read the installed version from `pip show`.

## See also

- [Home](Home)
- [Getting-Started](Getting-Started)
- [Architecture](Architecture)
- [Compatibility](Compatibility)
- [Sandbox-and-Testing](Sandbox-and-Testing)
- [Troubleshooting](Troubleshooting)
- [MagentoResource](MagentoResource)
- [Optional-Bridge](Optional-Bridge)
