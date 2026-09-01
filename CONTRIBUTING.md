# Contributing

This is a small library, so the process is deliberately light at the moment.

## Getting set up

```bash
git clone <repo>
cd geo-matcher
uv sync --all-groups
```

`--all-groups` pulls in the `linting`, `testing`, and `typechecking` dependency groups
alongside the runtime ones (see `pyproject.toml`).

## Before opening a PR

Run the same checks CI runs (`.github/workflows/ci.yml`):

```bash
uv run ruff check .
uv run mypy
uv run pytest -q
```

All three must pass on Python 3.11–3.13. `pytest -m unit` runs just the fast, offline
unit tests if you want a quicker inner loop — CI runs the full suite regardless.

## Code style

- Ruff (`ruff check`) and mypy (in gradual-strict mode — see `[tool.mypy]` in
  `pyproject.toml`) are enforced in CI; there's no separate style guide beyond what
  they check plus matching the surrounding code's naming, docstring, and comment
  density.
- Name things for what they do, not how they're implemented (e.g. `_national_spatial_join`,
  not `_national_single_phase`) — see the "Dispatchers" section of `geo_matcher/join.py`
  for the convention this follows.
- In the join engine (`join.py`), prefer PyArrow's compute layer
  (`pyarrow.compute`) over boxing values into Python objects/`set`s.
- Use British spelling in code, comments, and docs (e.g. "materialise", not "materialize").

## Branches & commits

Branch off `main` with a `feat/`, `fix/`, or `refactor/` prefix, and open a PR back into
`main`. Keep commits focused; the existing history is a reasonable style guide.

## Changelog

`CHANGELOG.md` follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Add an
entry under a new version heading (bump the `version` in `pyproject.toml` to match) for
anything user-visible — new CLI flags, behaviour changes, renamed public API. Purely
internal refactors with no behaviour change still deserve an entry if they touch public
names.

## Docs

- [How it works](docs/how-it-works.md) — join architecture and internals.
- [Usage](docs/usage.md) / [Output formats](docs/output.md) — user-facing reference.

Update these alongside any change to behaviour they describe.

## Licence

By contributing, you agree your contribution is made under the project's
[Apache 2.0 licence](LICENSE).
