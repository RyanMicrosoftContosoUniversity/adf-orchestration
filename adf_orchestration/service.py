import os
from typing import Optional, Dict

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
