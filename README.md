# `tap-dynamodb`

DynamoDB tap class.

Built with the [Meltano Singer SDK](https://sdk.meltano.com).

## Capabilities

* `catalog`
* `state`
* `discover`
* `about`
* `stream-maps`
* `schema-flattening`

## Settings

| Setting                 | Required | Default | Description |
|:------------------------|:--------:|:-------:|:------------|
| tables                  | False    | None    | An array of table names to extract from. |
| infer_schema_sample_size| False    |     100 | The amount of records to sample when inferring the schema. |
| table_scan_kwargs       | False    | None    | A mapping of table name to the scan kwargs that should be used to override the default when querying that table. |
| aws_access_key_id       | False    | None    | The access key for your AWS account. |
| aws_secret_access_key   | False    | None    | The secret key for your AWS account. |
| aws_session_token       | False    | None    | The session key for your AWS account. This is only needed when you are using temporary credentials. |
| aws_profile             | False    | None    | The AWS credentials profile name to use. The profile must be configured and accessible. |
| aws_default_region      | False    | None    | The default AWS region name (e.g. us-east-1)  |
| aws_endpoint_url        | False    | None    | The complete URL to use for the constructed client. |
| aws_assume_role_arn     | False    | None    | The role ARN to assume. |
| use_aws_env_vars        | False    |       0 | Whether to retrieve aws credentials from environment variables. |
| stream_maps             | False    | None    | Config object for stream maps capability. For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). |
| stream_map_config       | False    | None    | User-defined config values to be used within map expressions. |
| flattening_enabled      | False    | None    | 'True' to enable schema flattening and automatically expand nested properties. |
| flattening_max_depth    | False    | None    | The max depth to flatten schemas. |

A full list of supported settings and capabilities is available by running: `tap-dynamodb --about`

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

## Usage

You can easily run `tap-dynamodb` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-dynamodb --version
tap-dynamodb --help
tap-dynamodb --config CONFIG --discover > ./catalog.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

```bash
# Install pipx if you haven't already
pip install pipx
pipx ensurepath

# Restart your terminal here, if needed, to get the updated PATH
pipx install poetry

# Optional: Install Tox if you want to use it to run auto-formatters, linters, tests, etc.
pipx install tox
```

### Create and Run Tests

Create tests within the `tests` subfolder and
  then run:

```bash
pipx run tox -e pytest
pipx run tox -e pytest -- tests/test_dynamodb.py
```

You can also test the `tap-dynamodb` CLI interface directly using `poetry run`:

```bash
poetry run tap-dynamodb --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-dynamodb
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-dynamodb --version
# OR run a test `elt` pipeline:
meltano elt tap-dynamodb target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.
