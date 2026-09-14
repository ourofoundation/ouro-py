# Publishing

Releases publish to PyPI from GitHub Actions when a `vX.Y.Z` tag is pushed.
Trusted publishing is used — no API token.

```bash
make release              # bump patch in pyproject.toml
make release minor        # bump minor
make release major        # bump major

git add pyproject.toml
git commit -m "Bump version"
git tag v$(uv version --short)
git push origin HEAD --tags
```

The tag must match the version in `pyproject.toml` (`v0.11.17` for `0.11.17`).
Pushing it runs `.github/workflows/publish.yml`.

### One-time PyPI setup

On https://pypi.org/manage/project/ouro-py/settings/publishing add a GitHub
trusted publisher:

- Owner: `ourofoundation`
- Repository: `ouro-py`
- Workflow: `publish.yml`
- Environment: `pypi`
