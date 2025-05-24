# ADF Orchestration

This project provides a small utility service to interact with Azure Data Factory (ADF).

## Usage

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Define the following environment variables:
   - `SUBSCRIPTION_ID`
   - `RESOURCE_GROUP`
   - `FACTORY_NAME`
   - `AZURE_TENANT_ID`
   - `AZURE_CLIENT_ID`
   - `AZURE_CLIENT_SECRET`
3. Use `ADFService` to trigger pipelines:
   ```python
   from adf_orchestration import ADFService

   service = ADFService()
   run_id = service.trigger_pipeline("my_pipeline", parameters={"foo": "bar"})
   print(run_id)
   ```

## Testing

Run tests with `pytest`:
```bash
pytest
```
