# Publishing

Requires [uv](https://docs.astral.sh/uv/) and [keyring](https://pypi.org/project/keyring/).

```bash
make publish              # build current version and upload to PyPI
make release              # bump patch, then publish
make release minor        # bump minor, then publish
make release major        # bump major, then publish
```

### One-time token setup

1. Create a token at https://pypi.org/manage/account/token/ scoped to **ouro-py**
2. Store it in the macOS keychain:

```bash
keyring set 'https://upload.pypi.org/legacy/?ouro-py' __token__
# paste pypi-... when prompted
```

A `403 Forbidden` on upload usually means the token is expired, revoked, or scoped to a different project — generate a fresh token and re-run `keyring set`.

CI also publishes on GitHub Release publish once Trusted Publishing is configured on PyPI for workflow `python-publish.yml` / environment `pypi`.
