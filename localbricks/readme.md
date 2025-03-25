# Deus LocalBricks

LocalBricks is a custom module designed to facilitate local development and testing with Spark and Delta Lake using Docker in devs computers. It provides an easy way to run Spark locally, configure Databricks credentials, create Delta Shares, and manage local table configurations.

## Table of Contents
- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [Example](#example)

## Installation

### Prerequisites
- Databricks CLI installed and configured with personal developer token.
- Docker and Docker Compose installed on your machine.
- Databricks account allowed for Delta Sharing.

### Usage

<!-- 1. **Folder setup**
    Add a `dist` and `opt` folders in the root of this project, if not present. -->

2. **Run Docker Compose to set up Spark locally**
    ```sh
    docker-compose up
    ```

## Configuration

### Configure Databricks Share Credentials

You need to configure your Databricks share credentials to enable Delta Sharing via the [open sharing model](https://docs.databricks.com/en/data-sharing/read-data-open.html#before-you-begin).

<img width="1018" alt="image" src="https://github.com/Deusteam/Deusteam-pipeline_dev/assets/10424809/853ce3fd-8c8f-42b2-8521-0e15d858a337">


1. **Create your Databricks Share's recipient credentials**:
    Go to `"https://{DATABRICKS_HOST}/explore/sharing/shares?o={WORKSPACE_ID}"` and create a new share called `localbricks` (if not present), and add your personal recipient.
    Follow this video for details:
    https://github.com/Deusteam/Deusteam-pipeline_dev/assets/10424809/9b376678-bd50-4e54-a647-1f8b1b984a77

    Once you complete the share creation you should be able to activate your recipient downloading the file like the last step of the video (screenshot below). Copy the that `config.share` file and place it within this current working directory. The content should look like this:
    ```json
        {
            "shareCredentialsVersion": 1,
            "bearerToken": "<...>",
            "endpoint": "https://london.cloud.databricks.com/api/2.0/delta-sharing/metastores/<...>",
            "expirationTime": "9999-12-31T23:59:59.999Z"
        }
    ```
    <img width="809" alt="image" src="https://github.com/Deusteam/Deusteam-pipeline_dev/assets/10424809/df6b5faa-6bae-498c-b35f-f2da3d438818">

2. **Add the Delta Share to your local storage configuration**:

    If you want to store delta tables locally, you must update `local_tables_config.json` with your share information:
    ```json
        [
            {
                "share": "localbricks",
                "schema.table_name": "client-xyz.table_json_example"
            }
        ]
    ```

## Example

Below is a example python script that demonstrates how to use the `localbricks` data loader module. The module is composed of several main classes, each with its own functionality.
For more information how to read data from delta sharing using PySpark, please read the [docs here](https://docs.databricks.com/en/data-sharing/read-data-open.html#access-shared-data-using-spark).


```python
from localbricks_factory import data_loader_manager 

delta_pyspark_df = data_loader_manager(action='<target_action_here>', share='<share_name_here>', schema_table_name='<table_name_alias_here>')
print(delta_pyspark_df.show())
```

Make sure the `local_dev` docker container is up running, so now you can execute your `example.py` task locally in your computer by running the following command:
```sh
make localbricks_custom COMMAND="example.py"
```
