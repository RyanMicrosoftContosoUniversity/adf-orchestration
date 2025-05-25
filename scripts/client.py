#!/usr/bin/env python
"""
Script to interact with Azure Data Factory pipelines using ADFService.

Usage:
    python client.py list                              # List all pipelines
    python client.py run <pipeline_name> [key=value]   # Run a pipeline with optional parameters
    python client.py status <run_id>                  # Check status of a pipeline run
    python client.py cancel <run_id>                  # Cancel a pipeline run

Environment variables required:
    SUBSCRIPTION_ID: Azure subscription ID
    RESOURCE_GROUP: Resource group containing the Data Factory
    FACTORY_NAME: Name of the Data Factory
    AZURE_TENANT_ID: Azure tenant ID for authentication
    AZURE_CLIENT_ID: Client ID for authentication
    AZURE_CLIENT_SECRET: Client secret for authentication
"""

import sys
import os
from adf_orchestration.service import ADFService


def parse_parameters(param_strings):
    """Parse command-line parameters into a dictionary.
    
    Args:
        param_strings: List of strings in format "key=value"
    
    Returns:
        Dictionary of parameter key-value pairs
    """
    parameters = {}
    for param in param_strings:
        if '=' not in param:
            print(f"Warning: Ignoring malformed parameter '{param}'. Format should be key=value")
            continue
        key, value = param.split('=', 1)
        parameters[key] = value
    return parameters


def main():
    # Validate environment variables
    required_env_vars = [
        "SUBSCRIPTION_ID", "RESOURCE_GROUP", "FACTORY_NAME",
        "AZURE_TENANT_ID", "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET"
    ]
    
    missing_vars = [var for var in required_env_vars if var not in os.environ]
    if missing_vars:
        print(f"Error: Missing required environment variables: {', '.join(missing_vars)}")
        print("Please set these environment variables before running this script.")
        sys.exit(1)
    
    # Validate command line arguments
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    
    command = sys.argv[1].lower()
    
    try:
        # Initialize the ADF service
        service = ADFService()
        
        if command == "list":
            # List all pipelines
            pipelines = service.list_pipelines()
            print(f"Found {len(pipelines)} pipelines:")
            for i, pipeline in enumerate(pipelines, 1):
                folder = f" (folder: {pipeline['folder_name']})" if pipeline['folder_name'] else ""
                desc = f" - {pipeline['description']}" if pipeline['description'] else ""
                print(f"{i}. {pipeline['name']}{folder}{desc}")
                
        elif command == "run":
            if len(sys.argv) < 3:
                print("Error: Pipeline name required")
                print(__doc__)
                sys.exit(1)
                
            pipeline_name = sys.argv[2]
            parameters = parse_parameters(sys.argv[3:]) if len(sys.argv) > 3 else None
            
            # Trigger the pipeline
            print(f"Triggering pipeline '{pipeline_name}' with parameters: {parameters}")
            run_id = service.trigger_pipeline(pipeline_name, parameters)
            
            print(f"Pipeline triggered successfully. Run ID: {run_id}")
            print(f"You can monitor the pipeline run in the Azure Portal.")
            print(f"Or check status later with: python {sys.argv[0]} status {run_id}")
            
        elif command == "status":
            if len(sys.argv) < 3:
                print("Error: Run ID required")
                print(__doc__)
                sys.exit(1)
                
            run_id = sys.argv[2]
            
            # Get pipeline run status
            status = service.get_pipeline_run_status(run_id)
            
            print(f"Pipeline Run: {status['pipeline_name']}")
            print(f"Run ID: {status['run_id']}")
            print(f"Status: {status['status']}")
            print(f"Start time: {status['start_time']}")
            print(f"End time: {status['end_time'] or 'Still running'}")
            
            if status['end_time']:
                duration_sec = status['duration_in_ms'] / 1000
                print(f"Duration: {duration_sec:.2f} seconds")
            
            if status['parameters']:
                print("Parameters:")
                for key, value in status['parameters'].items():
                    print(f"  {key}: {value}")
                    
        elif command == "cancel":
            if len(sys.argv) < 3:
                print("Error: Run ID required")
                print(__doc__)
                sys.exit(1)
                
            run_id = sys.argv[2]
            
            # Cancel pipeline run
            print(f"Cancelling pipeline run with ID: {run_id}")
            service.cancel_pipeline_run(run_id)
            print("Cancel request submitted successfully")
            
        else:
            print(f"Unknown command: {command}")
            print(__doc__)
            sys.exit(1)
            
    except Exception as e:
        print(f"Error: Operation failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

