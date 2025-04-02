# Project Setup and Development Workflow

This guide outlines the setup process for the Databricks CLI, configuration of the development environment using Poetry & Docker, and the development workflow including code changes, package building, and deployment.

## Prerequisites

- Databricks CLI
- Poetry for Python package management

### Install Databricks CLI

Follow the Databricks documentation to install the CLI tool:

[Install Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/install.html)

### Configure Databricks CLI

After installation, run this command `databricks configure`, and  configure the Databricks CLI with your [personal access token (PAT)](https://docs.databricks.com/en/dev-tools/cli/authentication.html#id1) and the host of your Databricks workspace. Go to `"https://{DATABRICKS_HOST}/settings/user/developer/access-tokens?o={WORKSPACE_ID}"` and create the token. Usually the token looks like: `dapid12345689abcdxxx`. You can follow the image below for guidance:
<img width="1057" alt="image" src="https://github.com/Deusteam/Deusteam-pipeline_dev/assets/10424809/793fe39a-95d4-4c3c-b2a0-12800eee786a">

Run this command to see your Databricks CLI configuration:
```sh
cat ~/.databrickscfg
```


### Setup Poetry Environment

- Move to the root folder of the project
- Start a poetry venv: `poetry shell`
- Install dependencies: `poetry install`
- Verify packages installed: `poetry check`
- Update lock file: `poetry update` (util when the "deus_lib" wheel version has been rebuilded and updated)

## Development Workflow

- Code Modification: Use your preferred IDE to make changes to the code.
* (Try installing [VSCode extension](https://docs.databricks.com/en/dev-tools/vscode-ext/tutorial.html#step-3-install-the-databricks-extension) to sync code with your databricks workspace)
- Building: If changes are made within the `deus_lib` folder, rebuild the wheel using the command: `make build-wheel`
- Job Definitions: To create new jobs or modify existing ones, add or change YAML files in `workflow_definition/dds_jobs`
- Packing Jobs: After adding or modifying job definitions, make sure to pack the jobs: `make pack-jobs`
- Deployment: Deploy your changes to Databricks. For a default deployment use `make ci-pipeline-dev`, to deploy using a specific all-purpose cluster for development, use `make ci-pipeline-dev compute_id=[cluster_id]` (this is not supported yet)
- To run a job, execute the following command: `make run-job dds_code=[dds_code]`

## Project Structure

```
.
├── deus_lib/              # Core library with shared functionality
├── src/
│   └── tasks/
│       └── dds/          # DDS (Data Delivery Service) specific tasks
│           ├── common_behaviours.py  # Shared behavior calculations
│           ├── common_merge.py       # Common data merging utilities
│           ├── vs/                  # Aviation domain tasks
│           ├── st/                  # Maritime domain tasks (Shipping)
│           ├── chr/                 # Maritime domain tasks (Charter)
│           ├── ldc/                 # Maritime domain tasks (LDC)
│           ├── pga/                 # Maritime domain tasks (PGA)
│           └── cgl/                 # Maritime domain tasks (CGL)
├── tests/                # Test suites
└── workflow_definition/  # Job definitions and configurations
```

## Terminology

The project uses standardized terminology across all domains:

- `domain`: The industry sector (e.g., "aviation", "maritime")
- `dds_code`: Unique identifier for a data source (e.g., "vs", "st", "chr")
- `dds_name`: Human-readable name for a data source

## Configuration

Each DDS code requires specific configuration files:

1. `mapping.json`: Field mappings and data transformations
   - Defines field names, types, and options
   - Specifies catalog paths in format `dds-{code}`
   - Contains domain-specific behavior configurations

2. Job Configuration YAML:
   - Located in `workflow_definition/dds_jobs/`
   - Defines job parameters and dependencies
   - Specifies data processing steps

For detailed configuration guides and examples, see the documentation in `docs/`.
