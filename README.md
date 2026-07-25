# `ouro-py`

[![Version](https://img.shields.io/pypi/v/ouro-py?color=%2334D058)](https://pypi.org/project/ouro-py)
[![Python](https://img.shields.io/pypi/pyversions/ouro-py)](https://pypi.org/project/ouro-py)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg?label=license)](https://opensource.org/licenses/MIT)

The official Python SDK for [Ouro](https://ouro.foundation).

Use it to work with datasets and files, publish results, collaborate with teams, and run APIs
shared on Ouro.

## Install

```bash
pip install ouro-py
```

`ouro-py` requires Python 3.10 or later.

## Quickstart

Create a Personal Access Token in your
[Ouro settings](https://ouro.foundation/settings/api-keys), then export it:

```bash
export OURO_API_KEY="your-api-key"
```

Create a client and start using the API:

```python
from ouro import Ouro

ouro = Ouro()

dataset = ouro.datasets.create(
    name="experiment-results",
    visibility="private",
    data=[
        {"sample": "A", "score": 0.82},
        {"sample": "B", "score": 0.91},
    ],
)

results = ouro.datasets.query(dataset.id)
print(results)
```

The client reads `OURO_API_KEY` from your environment or a `.env` file. You can also pass it
directly with `Ouro(api_key="...")`.

## Common workflows

### Find assets

Datasets, posts, files, services, routes, and quests are all assets. Search across them with
`ouro.assets`:

```python
results = ouro.assets.search(
    "battery materials",
    asset_type="dataset",
    scope="global",
    limit=10,
)

asset = ouro.assets.retrieve(results[0]["id"])
```

Search scope can be `personal`, `org`, `global`, or `all`.

### Query a dataset

Dataset queries return pandas DataFrames by default:

```python
df = ouro.datasets.query(dataset_id)

summary = ouro.datasets.query(
    dataset_id,
    """
    select category, avg(score) as mean_score
    from {{table}}
    group by category
    """,
)
```

SQL queries are read-only. Use `{{table}}` as the dataset table placeholder.

### Upload a file

```python
file = ouro.files.create(
    name="Crystal structure",
    file_path="./structure.cif",
    visibility="private",
)
```

Download any supported asset through the shared asset interface:

```python
download = ouro.assets.download(file.id, output_path="./downloads")
print(download["path"])
```

### Publish a post

Pass markdown directly or build richer content with the editor:

```python
editor = ouro.posts.Editor()
editor.new_header(level=1, text="Experiment summary")
editor.new_paragraph(text="The best sample reached a score of 0.91.")
editor.new_inline_asset(dataset.id, asset_type="dataset", view_mode="preview")

post = ouro.posts.create(
    name="Experiment summary",
    content=editor,
    visibility="private",
)
```

You can also use `content_markdown="..."` or `content_path="./report.md"`.

### Run an API

Ouro services expose individual endpoints as routes:

```python
action = ouro.routes.execute(
    "organization/route-name",  # a route slug or UUID
    body={"text": "hello"},
)

print(action.status)
print(action.final_data)
```

Routes can take Ouro assets directly:

```python
action = ouro.routes.execute(
    route_id,
    input_assets={"structure": file.id},
)
```

Synchronous and asynchronous routes use the same interface. Pass `wait=False` to return
immediately, then use `ouro.routes.poll_action(action.id)` when you are ready for the result.

## API overview

Resources are organized under one client:

| Namespace | Use it for |
|---|---|
| `ouro.assets` | Search, retrieve, share, download, and inspect lineage |
| `ouro.datasets` | Create, query, update, and visualize tabular data |
| `ouro.files` | Upload, retrieve, update, and search files |
| `ouro.posts` | Publish markdown and embedded assets |
| `ouro.routes` | Execute APIs and inspect actions |
| `ouro.services` | Publish an API from an OpenAPI specification |
| `ouro.quests` | Create work, submit entries, and review results |
| `ouro.organizations` / `ouro.teams` | Manage workspaces, channels, and membership |
| `ouro.comments` / `ouro.conversations` | Discuss assets and send messages |
| `ouro.users` / `ouro.notifications` | Work with profiles and notifications |
| `ouro.money` | Check balances, transactions, and paid access |

See the [REST API reference](https://ouro.foundation/docs/developers/api) for the underlying API.

## Organizations, teams, and visibility

Every asset belongs to an organization and a team. When creating one, pass `org_id` and `team_id`
to choose where it appears:

```python
dataset = ouro.datasets.create(
    name="shared-results",
    data=rows,
    visibility="public",
    org_id=org_id,
    team_id=team_id,
)
```

If you omit them, Ouro uses your global organization's catch-all team.

Visibility can be `public`, `private`, or `monetized`. Private assets remain private until you
share them explicitly:

```python
ouro.assets.share(asset_id, user_id, role="read")
```

## Licensing and attribution

Set an asset's reuse terms with `license_id`, and record where the work came from with
the top-level `attribution` field. These fields are consistent across asset types and separate
from type-specific `metadata`:

```python
attribution = {
    "originality": "derivative",
    "github_url": "https://github.com/example/project",
    "doi_url": "https://doi.org/10.1234/example",
    "relation_type": "IsDerivedFrom",
}

dataset = ouro.datasets.create(
    name="published-results",
    visibility="public",
    data=rows,
    license_id="CC-BY-4.0",
    attribution=attribution,
)
```

`originality` can be `original`, `derivative`, or `third-party`. Attribution can also include
`paper_url` and `external_url`. Use `relation_type` to describe how the Ouro asset relates to the
linked work: `IsSupplementTo`, `IsDerivedFrom`, `References`, `IsVariantFormOf`, or
`IsIdenticalTo`.

Pass the same top-level fields when creating another asset type:

```python
service = ouro.services.create(
    name="published-model-api",
    base_url="https://api.example.com",
    license_id="Apache-2.0",
    attribution={
        "originality": "third-party",
        "github_url": "https://github.com/example/model",
        "paper_url": "https://arxiv.org/abs/0000.00000",
    },
)
```

Use the license that applies to the asset type and only publish third-party or derivative work
when its terms permit redistribution.

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `OURO_API_KEY` | required | Personal Access Token |
| `OURO_BACKEND_URL` | `https://api.ouro.foundation` | Ouro API base URL |

For local development:

```bash
export OURO_BACKEND_URL="http://localhost:8003"
```

You can also pass `api_key` and `base_url` directly to `Ouro(...)`.

## Error handling

All SDK exceptions inherit from `OuroError`:

```python
from ouro import NotFoundError, OuroError

try:
    asset = ouro.assets.retrieve(asset_id)
except NotFoundError:
    print("Asset not found")
except OuroError as exc:
    print(f"Ouro request failed: {exc}")
```

## Development

```bash
git clone git@github.com:ourofoundation/ouro-py.git
cd ouro-py
pip install -e .
pytest
```

Questions and ideas are welcome in
[GitHub Discussions](https://github.com/orgs/ourofoundation/discussions).

## License

MIT
