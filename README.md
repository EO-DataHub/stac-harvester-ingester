# stac-harvester-ingester

## Introduction
The STAC Harvester Ingester takes catalogue configuration data from a pulsar message and creates a STAC Harvester for 
each catalogue.

It is designed to operate as part of a data pipeline, receiving a pulsar message containing details about the 
required harvest and creating STAC Harvesters, event sources and sensors which will create jobs to harvest STAC data 
on schedule.


## Features

- **Creates STAC Harvester** - a Kubernetes Custom Resource containing details of harvest
  - URL
  - Source path
  - Target path
- **Creates sensors** - defines triggers for creation of a harvesting job
- **Creates event sources** - schedule for the sensor


## Getting started

### Prerequisites

- [uv](https://docs.astral.sh/uv/) - Python package and project manager

### Setup

```commandline
make setup
```

This will sync dependencies using uv and install pre-commit hooks.

It's safe and fast to run `make setup` repeatedly as it will only update these things if
they have changed.

After `make setup` you can run `pre-commit` to run pre-commit checks on staged changes and
`pre-commit run --all-files` to run them on all files. This replicates the linter checks that
run from GitHub actions.


## Configuration

The STAC Harvester Ingester is configured through environment variables

### Environment variables

- `STAC_HARVESTER_IMAGE_URL` - image for STAC harvesters
- `HARVESTER_TOPIC` - topic for pulsar messages
- `AWS_ACCESS_KEY` - AWS access key ID (optional)
- `AWS_SECRET_ACCESS_KEY` - AWS secret access key (optional)
- `TOPIC` - suffix for STAC pulsar messages (optional, default=stac)
- `PULSAR_URL` - URL of pulsar service


## Usage

The service is typically run as part of a data pipeline, but you can invoke it directly for testing or development.

Run the stac-harvester-ingester from the command line:

```sh
uv run python -m stac_harvester_ingester
```

## Development

- Code is in `stac_harvester_ingester`.
- Formatting: [Black](https://black.readthedocs.io/), [Ruff](https://docs.astral.sh/ruff/), [isort](https://pycqa.github.io/isort/).
- Linting: [Pylint](https://pylint.pycqa.org/).
- Pre-commit checks are installed with `make setup`.

Useful Makefile targets:

- `make setup`: Set up or update the dev environment.
- `make test`: Run tests continuously.
- `make testonce`: Run tests once.
- `make lint`: Run all linters and formatters.
- `make dockerbuild`: Build a Docker image.
- `make dockerpush`: Push a Docker image.


### Project Structure

- **__main__.py**: Reads in pulsar messages and creates sensors, event sources and STAC Harvesters


## Testing

Run all tests with:

```sh
make testonce
```

Tests use [pytest](https://docs.pytest.org/), [moto](https://github.com/spulec/moto) for AWS mocking, and [requests-mock](https://requests-mock.readthedocs.io/).


## Troubleshooting

- **Authentication errors:** Ensure your `AWS_ACCESS_KEY` and `AWS_SECRET_ACCESS_KEY` are set correctly and have permission to access the required S3 buckets.
- **Pulsar connection issues:** Check that `PULSAR_URL` is set to the correct broker address and is reachable from your environment.
- **S3 upload or download failures:** Verify that any relevant buckets exist, your credentials have the correct permissions, and the bucket region matches your configuration.

Check the application logs for detailed error messages.


## Release Process

The release process is fully automated and handled through GitHub Actions.  
On every push to `main` or when a new tag is created, the following checks and steps are run automatically:

- Pre-commit checks and linting
- Security scanning
- Unit tests
- Docker image build and push to the configured registry

Versioned releases are handled through the Releases page in github.

See [`.github/workflows/actions.yaml`](.github/workflows/actions.yaml) for details.


## License

This project is licensed under the United Kingdom Research and Innovation BSD Licence. See [LICENSE](LICENSE) for details.