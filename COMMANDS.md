# Publishing

Requires [uv](https://docs.astral.sh/uv/).

```bash
make publish              # build current version and upload to PyPI
make release              # bump patch, then publish
make release minor        # bump minor, then publish
make release major        # bump major, then publish
```

Set a PyPI API token (create at https://pypi.org/manage/account/token/, scoped to `ouro-py` or the whole account):

```bash
export UV_PUBLISH_TOKEN=pypi-...
```

A `403 Forbidden` on upload usually means the token is expired, revoked, or scoped to a different project — generate a fresh token.

CI also publishes on GitHub Release publish once Trusted Publishing is configured on PyPI for workflow `python-publish.yml` / environment `pypi`.
