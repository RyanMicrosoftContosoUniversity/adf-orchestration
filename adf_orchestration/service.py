import os
from typing import Optional, Dict, List, Any

from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient


class ADFService:
    """Service for interacting with Azure Data Factory."""

    def __init__(self) -> None:
        self.subscription_id = os.environ["SUBSCRIPTION_ID"]
        self.resource_group = os.environ["RESOURCE_GROUP"]
        self.factory_name = os.environ["FACTORY_NAME"]

        tenant_id = os.environ["AZURE_TENANT_ID"]
        client_id = os.environ["AZURE_CLIENT_ID"]
        client_secret = os.environ["AZURE_CLIENT_SECRET"]

        credential = ClientSecretCredential(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
        )

        self.client = DataFactoryManagementClient(credential, self.subscription_id)

    def trigger_pipeline(self, pipeline_name: str, parameters: Optional[Dict[str, str]] = None) -> str:
        """Trigger an existing Azure Data Factory pipeline.

        Args:
            pipeline_name: Name of the pipeline to run.
            parameters: Optional parameters to pass to the pipeline.

        Returns:
            The run ID of the triggered pipeline run.
        """

        run_response = self.client.pipelines.create_run(
            resource_group_name=self.resource_group,
            factory_name=self.factory_name,
            pipeline_name=pipeline_name,
            parameters=parameters or {},
        )
        return run_response.run_id

    def get_pipeline_run_status(self, run_id: str) -> Dict[str, Any]:
        """Get the status of a pipeline run.

        Args:
            run_id: The run ID returned by trigger_pipeline.

        Returns:
            Dictionary containing status information about the pipeline run.
        """
        run_response = self.client.pipeline_runs.get(
            resource_group_name=self.resource_group,
            factory_name=self.factory_name,
            run_id=run_id,
        )
        return {
            "run_id": run_response.run_id,
            "pipeline_name": run_response.pipeline_name,
            "status": run_response.status,
            "start_time": run_response.run_start,
            "end_time": run_response.run_end,
            "duration_in_ms": run_response.run_duration or 0,
            "parameters": run_response.parameters,
        }

    def list_pipelines(self) -> List[Dict[str, Any]]:
        """List all pipelines in the Azure Data Factory.

        Returns:
            List of dictionaries containing pipeline information.
        """
        pipelines = self.client.pipelines.list_by_factory(
            resource_group_name=self.resource_group,
            factory_name=self.factory_name,
        )

        result = []
        for pipeline in pipelines:
            result.append(
                {
                    "id": pipeline.id,
                    "name": pipeline.name,
                    "type": pipeline.type,
                    "description": pipeline.description,
                    "folder_name": pipeline.folder.name if pipeline.folder else None,
                }
            )
        return result

    def cancel_pipeline_run(self, run_id: str) -> None:
        """Cancel a pipeline run.

        Args:
            run_id: The run ID of the pipeline run to cancel.
        """
        self.client.pipeline_runs.cancel(
            resource_group_name=self.resource_group,
            factory_name=self.factory_name,
            run_id=run_id,
        )
