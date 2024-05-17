# `ouro-py`

Python client for [Ouro](https://ouro.foundation)

- Documentation: [ouro.foundation/docs](https://ouro.foundation/docs)

## Usage

Generate an API key from your account settings by going to [ouro.foundation/app/settings/api-keys](https://ouro.foundation/app/settings/api-keys).

Set your Ouro environment variables in a dotenv file, or using the shell:

```bash
export USER_API_KEY="your_ouro_api_key"
```

Init client:

```python
import os
from ouro import Ouro

api_key = os.environ.get("USER_API_KEY")
ouro = Ouro()
ouro.login(api_key)
```

Use the client to interface with the Ouro framework.

### Create a dataset

```python
data = pd.DataFrame([
    {"name": "Bob", "age": 30},
    {"name": "Alice", "age": 27},
    {"name": "Matt", "age": 26},

])

dataset = ouro.earth.create_dataset({
        "name": "unique_dataset_name",
        "description": "dataset_description",
        "visibility": "private",
    },
    data
)
```

### Read a dataset

```python
id = "3d82308b-0747-45e4-8045-c8f7d2f6c0a6" # penguins dataset
dataset = ouro.earth.read_dataset(id)

# Read dataset's data as a Pandas DataFrame
df = ouro.earth.read_dataset_data(id)
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
[![Version](https://img.shields.io/pypi/v/ouro-py?color=%2334D058)](https://pypi.org/project/ouro-py)
[![Codecov](https://codecov.io/gh/ourofoundation/ouro-py/branch/develop/graph/badge.svg)](https://codecov.io/gh/ourofoundation/ouro-py)
[![Last commit](https://img.shields.io/github/last-commit/ourofoundation/ouro-py.svg?style=flat)](https://github.com/ourofoundation/ouro-py/commits)
[![GitHub commit activity](https://img.shields.io/github/commit-activity/m/ourofoundation/ouro-py)](https://github.com/ourofoundation/ouro-py/commits)
[![Github Stars](https://img.shields.io/github/stars/ourofoundation/ouro-py?style=flat&logo=github)](https://github.com/ourofoundation/ouro-py/stargazers)
[![Github Forks](https://img.shields.io/github/forks/ourofoundation/ouro-py?style=flat&logo=github)](https://github.com/ourofoundation/ouro-py/network/members)
[![Github Watchers](https://img.shields.io/github/watchers/ourofoundation/ouro-py?style=flat&logo=github)](https://github.com/ourofoundation/ouro-py)
[![GitHub contributors](https://img.shields.io/github/contributors/ourofoundation/ouro-py)](https://github.com/ourofoundation/ouro-py/graphs/contributors)
