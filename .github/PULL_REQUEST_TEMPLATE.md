## Summary

Describe the change in 2 or 3 bullets.

## Why

Explain the problem this PR solves and why this approach was chosen.

## Changes

- [ ] Code updated
- [ ] Tests added or updated (if behavior changed)
- [ ] Documentation updated (`README.md`, `CHANGELOG.md` for user-visible changes)
- [ ] `AGENTS.md` updated (if a convention, boundary or command changed)

## Validation

Paste exact commands and outcomes (do not claim verified without evidence):

```bash
.venv/bin/pytest -q
.venv/bin/pytest -m samples -q
# live (maintainers, sandbox up):
eval "$(scripts/sandbox.sh env)"
MAGENTO_CA_BUNDLE=~/.govard/ssl/root.crt .venv/bin/pytest -m live -q
```

## Linked Issues

Fixes #

## Checklist

- [ ] Branch is `feat/...`, `fix/...`, `docs/...` or `chore/...` off `master` (no direct commits to `master`)
- [ ] Commits follow Conventional Commits (`feat:`, `fix:`, `docs:`, `chore:`), imperative mood
- [ ] Rebased on the current `master` rather than merged
- [ ] No private project names, local paths or credentials in code, docs or commit messages (public repo)
- [ ] `MagentoResource` stays a generic REST client: no project-specific companion-module endpoint
- [ ] The suite is green, and no live assertion was weakened to get there
