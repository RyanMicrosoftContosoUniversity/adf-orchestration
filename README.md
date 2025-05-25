# ADF Orchestration

This project provides a utility service and client script to interact with Azure Data Factory (ADF).

## Features

- Trigger Azure Data Factory pipelines with custom parameters
- Get pipeline run status and details
- List all available pipelines in the Azure Data Factory instance
- Cancel running pipeline executions

## Installation

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Configuration

Define the following environment variables:
- `SUBSCRIPTION_ID`: Azure subscription ID
- `RESOURCE_GROUP`: Resource group containing the Data Factory
- `FACTORY_NAME`: Name of the Azure Data Factory
- `AZURE_TENANT_ID`: Azure tenant ID for authentication
- `AZURE_CLIENT_ID`: Client ID for Service Principal authentication
- `AZURE_CLIENT_SECRET`: Client secret for Service Principal authentication

## Usage

### Using the Python API

```python
from adf_orchestration import ADFService

# Initialize the service
service = ADFService()

# Trigger a pipeline with parameters
run_id = service.trigger_pipeline("my_pipeline", parameters={"param1": "value1"})
print(f"Pipeline triggered with run ID: {run_id}")

# Get status of a pipeline run
status = service.get_pipeline_run_status(run_id)
print(f"Pipeline status: {status['status']}")

# List all available pipelines
pipelines = service.list_pipelines()
for pipeline in pipelines:
    print(f"Pipeline name: {pipeline['name']}")

# Cancel a pipeline run
service.cancel_pipeline_run(run_id)
```

### Using the Command-Line Tool

```bash
# List all pipelines in the Data Factory
python scripts/client.py list

# Run a pipeline with parameters
python scripts/client.py run my_pipeline param1=value1 param2=value2

# Check status of a pipeline run
python scripts/client.py status <run_id>

# Cancel a running pipeline
python scripts/client.py cancel <run_id>
```

## Testing

Run tests with `pytest`:
```bash
pytest
```
