import sys
import os
from types import SimpleNamespace, ModuleType

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest

# Provide a lightweight stub for azure.identity so tests do not require the real
# package which may not be installed in the execution environment.
azure_identity_stub = ModuleType("azure.identity")

class DummyCredential:
    def __init__(self, *args, **kwargs):
        pass

azure_identity_stub.ClientSecretCredential = DummyCredential
sys.modules.setdefault("azure.identity", azure_identity_stub)

from adf_orchestration import ADFService


class DummyPipelineRuns:
    def create_run(self, resource_group_name, factory_name, pipeline_name, parameters):
        self.called_with = {
            "resource_group_name": resource_group_name,
            "factory_name": factory_name,
            "pipeline_name": pipeline_name,
            "parameters": parameters,
        }
        return SimpleNamespace(run_id="dummy-run-id")


class DummyClient:
    def __init__(self, *args, **kwargs):
        self.pipelines = DummyPipelineRuns()


def test_service_initialization(monkeypatch):
    env = {
        "SUBSCRIPTION_ID": "sub",
        "RESOURCE_GROUP": "rg",
        "FACTORY_NAME": "factory",
        "AZURE_TENANT_ID": "tenant",
        "AZURE_CLIENT_ID": "client",
        "AZURE_CLIENT_SECRET": "secret",
    }
    monkeypatch.setattr(os, "environ", env)

    dummy_client = DummyClient()

    def dummy_client_factory(credential, subscription_id):
        assert subscription_id == "sub"
        return dummy_client

    monkeypatch.setattr(
        "adf_orchestration.service.DataFactoryManagementClient", dummy_client_factory
    )

    service = ADFService()
    assert service.client is dummy_client
    assert service.resource_group == "rg"
    assert service.factory_name == "factory"


def test_trigger_pipeline(monkeypatch):
    env = {
        "SUBSCRIPTION_ID": "sub",
        "RESOURCE_GROUP": "rg",
        "FACTORY_NAME": "factory",
        "AZURE_TENANT_ID": "tenant",
        "AZURE_CLIENT_ID": "client",
        "AZURE_CLIENT_SECRET": "secret",
    }
    monkeypatch.setattr(os, "environ", env)

    dummy_client = DummyClient()

    def dummy_client_factory(credential, subscription_id):
        return dummy_client

    monkeypatch.setattr(
        "adf_orchestration.service.DataFactoryManagementClient", dummy_client_factory
    )

    service = ADFService()
    run_id = service.trigger_pipeline("my-pipeline", parameters={"p1": "v1"})

    assert run_id == "dummy-run-id"
    assert dummy_client.pipelines.called_with == {
        "resource_group_name": "rg",
        "factory_name": "factory",
        "pipeline_name": "my-pipeline",
        "parameters": {"p1": "v1"},
    }
