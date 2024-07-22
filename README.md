# `ouro-py`

[![Version](https://img.shields.io/pypi/v/ouro-py?color=%2334D058)](https://pypi.org/project/ouro-py)

The Ouro Python library provides convenient access to the Ouro REST API from any Python 3.7+
application. Visit [Ouro](https://ouro.foundation) to learn more about the Ouro platform.

## Documentation

The REST API documentation can be found on [ouro.foundation/docs](https://ouro.foundation/docs).

## Installation

```sh
# install from PyPI
pip install ouro-py
```

## Usage

Generate an API key from your account settings by going to [ouro.foundation/settings/api-keys](https://ouro.foundation/settings/api-keys).

Set your Ouro environment variables in a dotenv file, or using the shell:

```bash
export USER_API_KEY="your_ouro_api_key"
```

Init client:

```python
import os
from ouro import Ouro

api_key = os.environ.get("USER_API_KEY")
ouro = Ouro(api_key=api_key)
```

Use the client to interface with the Ouro framework.

### Create a dataset

```python
data = pd.DataFrame(
    [
        {"name": "Bob", "age": 30},
        {"name": "Alice", "age": 27},
        {"name": "Matt", "age": 26},
    ]
)

res = ouro.elements.earth.datasets.create(
    data=data,
    name="your-dataset-name",
    description="your-dataset-description",
    visibility="private",
)
```

### Read a dataset

```python
id = "3d82308b-0747-45e4-8045-c8f7d2f6c0a6" # penguins dataset

# Retrieve a dataset
dataset = ouro.elements.earth.datasets.retrieve(id)

# Option 1: Load dataset's data as json using the table name
data = ouro.elements.earth.datasets.load("penguins")

# Option 2: Load dataset's data using tbe Postgrest client
data = ouro.database.table("penguins").select("*").limit(1).execute()

# Option 3: Read dataset's data as a Pandas DataFrame
df = ouro.elements.earth.datasets.query(id)
```

### Update a dataset

```python
id = "3d82308b-0747-45e4-8045-c8f7d2f6c0a6"
data_update = pd.DataFrame([
    {"name": "Bob", "age": 30},
    {"name": "Alice", "age": 27},
    {"name": "Matt", "age": 26},
])

update = {
    "visibility": "private",
    "data": data_update,
}
data = ouro.elements.earth.datasets.update("018f86da-b1be-7099-9556-fe88fb6882c3", **update)
```


### Create a post

```python
content = ouro.elements.air.Editor()
content.new_header(level=1, text="Hello World")
content.new_paragraph(text="This is a paragraph written in code.")

post = ouro.elements.air.posts.create(
    content=content,
    name="Hello World",
    description="This is a post from the Python SDK",
    visibility="private",
)
```

### Read a post

```python
id = "b9ff1bfd-b3ae-4e92-9afc-70b1e1e2011a" # The post id

post = ouro.elements.air.posts.retrieve(id)
```

### Update a post

```python
id = "b9ff1bfd-b3ae-4e92-9afc-70b1e1e2011a" # The post id

new_content = ouro.elements.air.Editor()
new_content.new_header(level=1, text="Hello World")
new_content.new_paragraph(text="This is a paragraph, but different this time.")

update = {
    "visibility": "public",
    "content": new_content,
}
post = ouro.elements.air.posts.update(id, **update)
```


## Contributing

Contributing to the Python library is a great way to get involved with the Ouro community. Reach out to us on our [Github Discussions](https://github.com/orgs/ourofoundation/discussions) page if you want to get involved.

## Set up a Local Development Environment

### Clone the Repository

```bash
git clone git@github.com:ourofoundation/ouro-py.git
cd ouro-py
```

### Create and Activate a Virtual Environment

We recommend activating your virtual environment. Click [here](https://docs.python.org/3/library/venv.html) for more about Python virtual environments and working with [conda](https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#activating-an-environment) and [poetry](https://python-poetry.org/docs/basic-usage/).

Using venv (Python 3 built-in):

```bash
python3 -m venv env
source env/bin/activate  # On Windows, use .\env\Scripts\activate
```

Using conda:

```bash
conda create --name ouro-py
conda activate ouro-py
```

### PyPi installation

Install the package (for > Python 3.7):

```bash
# with pip
pip install ouro-py
```

### Local installation

You can also install locally after cloning this repo. Install Development mode with `pip install -e`, which makes it so when you edit the source code the changes will be reflected in your python module.

## Badges

[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg?label=license)](https://opensource.org/licenses/MIT)
[![CI](https://github.com/ourofoundation/ouro-py/actions/workflows/ci.yml/badge.svg)](https://github.com/ourofoundation/ouro-py/actions/workflows/ci.yml)
[![Python](https://img.shields.io/pypi/pyversions/ouro-py)](https://pypi.org/project/ouro-py)

[![Codecov](https://codecov.io/gh/ourofoundation/ouro-py/branch/develop/graph/badge.svg)](https://codecov.io/gh/ourofoundation/ouro-py)
[![Last commit](https://img.shields.io/github/last-commit/ourofoundation/ouro-py.svg?style=flat)](https://github.com/ourofoundation/ouro-py/commits)
[![GitHub commit activity](https://img.shields.io/github/commit-activity/m/ourofoundation/ouro-py)](https://github.com/ourofoundation/ouro-py/commits)
[![Github Stars](https://img.shields.io/github/stars/ourofoundation/ouro-py?style=flat&logo=github)](https://github.com/ourofoundation/ouro-py/stargazers)
[![Github Forks](https://img.shields.io/github/forks/ourofoundation/ouro-py?style=flat&logo=github)](https://github.com/ourofoundation/ouro-py/network/members)
[![Github Watchers](https://img.shields.io/github/watchers/ourofoundation/ouro-py?style=flat&logo=github)](https://github.com/ourofoundation/ouro-py)
[![GitHub contributors](https://img.shields.io/github/contributors/ourofoundation/ouro-py)](https://github.com/ourofoundation/ouro-py/graphs/contributors)
